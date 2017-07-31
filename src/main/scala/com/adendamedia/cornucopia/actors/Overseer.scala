package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ReshardTableNew.ReshardTableType
import com.adendamedia.cornucopia.Config
import com.adendamedia.cornucopia.redis.ClusterOperations.{RedisUriToNodeId, ClusterConnectionsType}
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

import scala.concurrent.duration._
import scala.util.Try

object Overseer {
  def props(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
            reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
            clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
            clusterReadySupervisorMaker: ActorRefFactory => ActorRef,
            migrateSlotSupervisorMaker: ActorRefFactory => ActorRef,
            replicatePoorestMasterSupervisorMaker: ActorRefFactory => ActorRef,
            failoverSupervisorMaker: ActorRefFactory => ActorRef,
            getSlavesOfMasterSupervisorMaker: ActorRefFactory => ActorRef,
            forgetRedisNodeSupervisorMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations): Props =
    Props(new Overseer(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, clusterConnectionsSupervisorMaker,
      clusterReadySupervisorMaker, migrateSlotSupervisorMaker, replicatePoorestMasterSupervisorMaker,
      failoverSupervisorMaker, getSlavesOfMasterSupervisorMaker, forgetRedisNodeSupervisorMaker)
    )

  val name = "overseer"

  trait OverseerCommand

  trait NodeAddedEvent {
    val uri: RedisURI
  }

  trait NodeRemovedEvent {
    val uri: RedisURI
  }

  trait JoinNode extends OverseerCommand {
    val redisURI: RedisURI
  }

  case class JoinMasterNode(redisURI: RedisURI) extends JoinNode
  case class JoinSlaveNode(redisURI: RedisURI) extends JoinNode

  case class MasterNodeAdded(uri: RedisURI) extends NodeAddedEvent
  case class SlaveNodeAdded(uri: RedisURI) extends NodeAddedEvent

  case class MasterNodeRemoved(uri: RedisURI) extends NodeRemovedEvent
  case class SlaveNodeRemoved(uri: RedisURI) extends NodeRemovedEvent

  trait NodeJoinedEvent {
    val uri: RedisURI
  }
  case class MasterNodeJoined(uri: RedisURI) extends NodeJoinedEvent
  case class SlaveNodeJoined(uri: RedisURI) extends NodeJoinedEvent

  trait Reshard extends OverseerCommand
  case class ReshardWithNewMaster(uri: RedisURI) extends Reshard
  case class ReshardWithoutRetiredMaster(uri: RedisURI) extends Reshard

  case class GetClusterConnections(newRedisUri: RedisURI) extends OverseerCommand
  case class GotClusterConnections(connections: (ClusterOperations.ClusterConnectionsType,ClusterOperations.RedisUriToNodeId))

  case class GotReshardTable(reshardTable: ReshardTableType)

  case class KillChild(command: OverseerCommand, reason: Option[Throwable] = None)

  case class WaitForClusterToBeReady(connections: ClusterOperations.ClusterConnectionsType) extends OverseerCommand
  case object ClusterIsReady
  case object ClusterNotReady

  case class MigrateSlotsForNewMaster(newMasterUri: RedisURI, connections: ClusterOperations.ClusterConnectionsType,
                                      redisUriToNodeId: RedisUriToNodeId,
                                      reshardTable: ReshardTableType) extends OverseerCommand

  case class MigrateSlotsWithoutRetiredMaster(retiredMasterUri: RedisURI, connections: ClusterOperations.ClusterConnectionsType,
                                              redisUriToNodeId: RedisUriToNodeId,
                                              reshardTable: ReshardTableType) extends OverseerCommand

  case class JobCompleted(job: OverseerCommand)

  case object Reset extends OverseerCommand

  case class ValidateConnections(msg: GetClusterConnections, connections: (ClusterConnectionsType, RedisUriToNodeId)) extends OverseerCommand
  case object ClusterConnectionsValid
  case object ClusterConnectionsInvalid

  case class ReplicatePoorestMasterUsingSlave(slaveUri: RedisURI,
                                              connections: ClusterOperations.ClusterConnectionsType,
                                              redisUriToNodeId: RedisUriToNodeId) extends OverseerCommand
  case class ReplicatePoorestRemainingMasterUsingSlave(slaveUri: RedisURI,
                                                       excludedMasters: List[RedisURI],
                                                       connections: ClusterOperations.ClusterConnectionsType,
                                                       redisUriToNodeId: RedisUriToNodeId) extends OverseerCommand
  case class ReplicateMaster(slaveUri: RedisURI, masterNodeId: ClusterOperations.NodeId,
                             connections: ClusterConnectionsType, redisUriToNodeId: RedisUriToNodeId,
                             ref: ActorRef) extends OverseerCommand
  case class ReplicatedMaster(newSlaveUri: RedisURI)

  trait FailoverCommand extends OverseerCommand {
    val uri: RedisURI
  }
  /**
    * Used when removing a master
     * @param uri The URI of the redis node that should become a master if necessary
    */
  case class FailoverMaster(uri: RedisURI) extends FailoverCommand

  /**
    * Used when removing a slave
    * @param uri The URI of the redis node that should become a slave if necessary
    */
  case class FailoverSlave(uri: RedisURI) extends FailoverCommand

  case object FailoverComplete

  /**
    * All other nodes should forget this node. Either a slave node, or a master without any slaves.
    * @param uri The URI of the node to forget from the cluster
    */
  case class ForgetNode(uri: RedisURI) extends OverseerCommand

  /**
    * Event signalling that the node asked to be forgotten was forgotten
    * @param uri The URI of the forgotten node
    */
  case class NodeForgotten(uri: RedisURI)

  /**
    * Command for retrieving all the slaves of the given master
    * @param masterUri The URI of the master to get slaves of
    */
  case class GetSlavesOf(masterUri: RedisURI) extends OverseerCommand

  case class GotSlavesOf(masterUri: RedisURI, slaves: List[RedisClusterNode])
}

/**
  * The overseer subscribes to Redis commands that have been published by the dispatcher. This actor is the parent
  * actor of all actors that process Redis cluster commands. Cluster commands include adding and removing nodes. The
  * overseer subscribes to the Shutdown message, whereby after receiving this message, it will restart its children.
  */
class Overseer(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
               reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
               clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
               clusterReadySupervisorMaker: ActorRefFactory => ActorRef,
               migrateSlotSupervisorMaker: ActorRefFactory => ActorRef,
               replicatePoorestMasterSupervisorMaker: ActorRefFactory => ActorRef,
               failoverSupervisorMaker: ActorRefFactory => ActorRef,
               getSlavesOfMasterSupervisorMaker: ActorRefFactory => ActorRef,
               forgetRedisNodeSupervisorMaker: ActorRefFactory => ActorRef)
              (implicit clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import MessageBus.{AddNode, AddMaster, AddSlave, Shutdown, FailedAddingMasterRedisNode, RemoveNode, RemoveMaster}
  import Overseer._
  import ClusterOperations.ClusterConnectionsType

  import context.dispatcher

  val joinRedisNodeSupervisor: ActorRef = joinRedisNodeSupervisorMaker(context)
  val reshardClusterSupervisor: ActorRef = reshardClusterSupervisorMaker(context)
  val clusterConnectionsSupervisor: ActorRef = clusterConnectionsSupervisorMaker(context)
  val clusterReadySupervisor: ActorRef = clusterReadySupervisorMaker(context)
  val migrateSlotsSupervisor: ActorRef = migrateSlotSupervisorMaker(context)
  val replicatePoorestMasterSupervisor: ActorRef = replicatePoorestMasterSupervisorMaker(context)
  val failoverSupervisor: ActorRef = failoverSupervisorMaker(context)
  val getSlavesOfMasterSupervisor: ActorRef = getSlavesOfMasterSupervisorMaker(context)
  val forgetRedisNodeSupervisor: ActorRef = forgetRedisNodeSupervisorMaker(context)

  context.system.eventStream.subscribe(self, classOf[AddNode])
  context.system.eventStream.subscribe(self, classOf[RemoveNode])
  context.system.eventStream.subscribe(self, classOf[Shutdown])

  override def supervisorStrategy = OneForOneStrategy() {
    case e: FailedAddingRedisNodeException =>
      log.error(s"${e.message}: Restarting child actor")
      context.system.eventStream.publish(FailedAddingMasterRedisNode(e.message))
      Restart
  }

  override def receive: Receive = acceptingCommands

  private def acceptingCommands: Receive = {
    case m: AddMaster =>
      log.info(s"Received message AddMaster(${m.uri})")
      joinRedisNodeSupervisor ! JoinMasterNode(m.uri)
      context.become(joiningNode(m.uri))
    case s: AddSlave =>
      log.info(s"Received message AddSlave(${s.uri})")
      joinRedisNodeSupervisor ! JoinSlaveNode(s.uri)
      context.become(joiningNode(s.uri))
    case rm: RemoveMaster =>
      log.info(s"Received message RemoveMaster(${rm.uri})")
      val msg = FailoverMaster(rm.uri)
      failoverSupervisor ! msg
      context.become(failingOverForRemovingMaster(rm.uri))
  }

  private def failingOverForRemovingMaster(uri: RedisURI): Receive = {
    case FailoverComplete =>
      log.info(s"Failover completed successfully, $uri is now a master node")
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      reshardClusterSupervisor ! ReshardWithoutRetiredMaster(uri)
      context.become(computingReshardTableForRemovingMaster(uri))
  }

  private def computingReshardTableForRemovingMaster(retiredMaster: RedisURI,
                                                     clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId)] = None,
                                                     reshardTable: Option[ReshardTableType] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections for removing retired master $retiredMaster")
      reshardTable match {
        case Some(table) =>
          val cmd = MigrateSlotsWithoutRetiredMaster(retiredMaster, connections._1, connections._2, table)
          migrateSlotsSupervisor ! cmd
          context.become(migratingSlotsWithoutRetiredMaster(cmd))
        case None =>
          context.become(computingReshardTableForRemovingMaster(retiredMaster, Some(connections), None))
      }
    case GotReshardTable(table) =>
      log.info(s"Got reshard table for removing retired master $retiredMaster")
      clusterConnections match {
        case Some(connections) =>
          val cmd = MigrateSlotsWithoutRetiredMaster(retiredMaster, connections._1, connections._2, table)
          migrateSlotsSupervisor ! cmd
          context.become(migratingSlotsWithoutRetiredMaster(cmd))
        case None =>
          context.become(computingReshardTableForRemovingMaster(retiredMaster, None, Some(table)))
      }
  }

  private def joiningNode(uri: RedisURI): Receive = {
    case masterNodeJoined: MasterNodeJoined =>
      log.info(s"Master Redis node ${masterNodeJoined.uri} successfully joined")
      reshardClusterSupervisor ! ReshardWithNewMaster(uri)
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      context.become(reshardingWithNewMaster(uri))
    case slaveNodeJoined: SlaveNodeJoined =>
      log.info(s"Slave Redis node ${slaveNodeJoined.uri} successfully joined")
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      context.become(addingSlaveNode(slaveNodeJoined.uri))
  }

  private def addingSlaveNode(uri: RedisURI,
                              clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId)] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections")
      val masterConnections = connections._1
      val redisUriToNodeId = connections._2
      replicatePoorestMasterSupervisor ! ReplicatePoorestMasterUsingSlave(uri, masterConnections, redisUriToNodeId)
      context.become(addingSlaveNode(uri, Some(connections)))
    case ReplicatedMaster(slaveUri) =>
      log.info(s"Successfully replicated master node by new slave node $uri")
      context.system.eventStream.publish(SlaveNodeAdded(slaveUri))
      context.become(acceptingCommands)
  }

  /**
    * Computing the reshard table and getting the cluster connections is done concurrently
    * @param uri The Redis URI of the new master being added
    * @param reshardTable The compute reshard table
    * @param clusterConnections The connections to the master nodes
    */
  private def reshardingWithNewMaster(uri: RedisURI, reshardTable: Option[ReshardTableType] = None,
                                      clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId)] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections")
      reshardTable match {
        case Some(table) =>
          if (connectionsAreValidForAddingNewMaster(table, connections, uri)) {
            clusterReadySupervisor ! WaitForClusterToBeReady(connections._1)
            context.become(waitingForClusterToBeReadyForNewMaster(uri, table, connections))
          }
          else {
            log.warning(s"Redis connections are not valid")
            context.system.scheduler.scheduleOnce(2 seconds) {
              clusterConnectionsSupervisor ! GetClusterConnections(uri)
            }
            context.become(reshardingWithNewMaster(uri, Some(table), None)) // discard invalid connections
          }
        case None =>
          context.become(reshardingWithNewMaster(uri, None, Some(connections)))
      }
    case GotReshardTable(table) =>
      log.info(s"Got rehard table")
      clusterConnections match {
        case Some(connections) =>
          if (connectionsAreValidForAddingNewMaster(table, connections, uri)) {
            clusterReadySupervisor ! WaitForClusterToBeReady(connections._1)
            context.become(waitingForClusterToBeReadyForNewMaster(uri, table, connections))
          }
          else {
            log.warning(s"Redis connections are not valid")
            context.system.scheduler.scheduleOnce(2 seconds) {
              clusterConnectionsSupervisor ! GetClusterConnections(uri)
            }
            context.become(reshardingWithNewMaster(uri, Some(table), None)) // discard invalid connections
          }
        case None =>
          context.become(reshardingWithNewMaster(uri, Some(table), None))
      }
  }

  private def connectionsAreValidForAddingNewMaster(table: ReshardTableType,
                                                    connections: (ClusterConnectionsType, RedisUriToNodeId),
                                                    newRedisUri: RedisURI) = {
    // Since we validated the number of slots in the reshard table, we can be sure that it has all the master NodeId's in it
    val numNodes = table.keys.count(_ => true)
    val numConnections = connections._1.keys.count(_ != connections._2(newRedisUri.toString))
    numNodes == numConnections
  }

  private def waitingForClusterToBeReadyForNewMaster(uri: RedisURI, reshardTable: ReshardTableType,
                                                     connections: (ClusterConnectionsType, RedisUriToNodeId)): Receive = {
    case ClusterIsReady =>
      log.info(s"Cluster is ready, migrating slots")
      val msg = MigrateSlotsForNewMaster(uri, connections._1, connections._2, reshardTable)
      migrateSlotsSupervisor ! msg
      context.become(migratingSlotsForNewMaster(msg))
  }

  private def migratingSlotsForNewMaster(overseerCommand: OverseerCommand): Receive = {
    case JobCompleted(job: MigrateSlotsForNewMaster) =>
      log.info(s"Successfully added master node ${job.newMasterUri.toURI}")
      context.system.eventStream.publish(MasterNodeAdded(job.newMasterUri))
      context.become(acceptingCommands)
  }

  private def migratingSlotsWithoutRetiredMaster(overseerCommand: OverseerCommand): Receive = {
    case JobCompleted(job: MigrateSlotsWithoutRetiredMaster) =>
      log.info(s"Successfully resharded without retired master node ${job.retiredMasterUri.toURI}")
      val cmd = GetSlavesOf(job.retiredMasterUri)
      getSlavesOfMasterSupervisor ! cmd
      context.become(gettingSlavesOfMaster(job.retiredMasterUri, cmd, job.connections, job.redisUriToNodeId))
    case _ =>
      log.error("wat42")
  }

  private def gettingSlavesOfMaster(retiredMasterUri: RedisURI, command: GetSlavesOf,
                                    connections: ClusterConnectionsType,
                                    redisUriToNodeId: RedisUriToNodeId): Receive = {
    case event: GotSlavesOf =>
      log.info(s"Got slaves of retired master $retiredMasterUri: ${event.slaves.map(_.getUri.toURI)}")

      if (event.slaves.isEmpty) {
        val cmd = ForgetNode(retiredMasterUri)
        forgetRedisNodeSupervisor ! cmd
        context.become(forgettingNode(cmd, retiredMasterUri))
      }

      val excludedMaster = List(retiredMasterUri)
      event.slaves foreach { slave =>
        val msg = ReplicatePoorestRemainingMasterUsingSlave(slave.getUri, excludedMaster, connections, redisUriToNodeId)
        replicatePoorestMasterSupervisor ! msg
      }
      val slaves = event.slaves.map(_.getUri)
      context.become(
        replicatingPoorestRemainingMaster(retiredMasterUri, slaves, excludedMaster, connections, redisUriToNodeId)
      )
  }

  private def replicatingPoorestRemainingMaster(retiredMasterUri: RedisURI,
                                                remainingSlaves: List[RedisURI],
                                                excludedMasters: List[RedisURI],
                                                connections: ClusterConnectionsType,
                                                redisUriToNodeId: RedisUriToNodeId): Receive = {
    case ReplicatedMaster(uri) =>
      log.info(s"Successfully replicated master node by new slave node $uri")
      remainingSlaves match {
        case x :: xs =>
          val msg = ReplicatePoorestRemainingMasterUsingSlave(x, excludedMasters, connections, redisUriToNodeId)
          replicatePoorestMasterSupervisor ! msg
          context.become(
            replicatingPoorestRemainingMaster(retiredMasterUri, xs, excludedMasters, connections, redisUriToNodeId)
          )
        case Nil =>
          val cmd = ForgetNode(retiredMasterUri)
          forgetRedisNodeSupervisor ! cmd
          context.become(forgettingNode(cmd, retiredMasterUri))
      }
  }

  private def forgettingNode(overseerCommand: OverseerCommand, uri: RedisURI): Receive = {
    case event: NodeForgotten =>
      log.info(s"Successfully forgot node ${event.uri}")
      val msg = MasterNodeRemoved(uri)
      context.system.eventStream.publish(msg)
      context.become(acceptingCommands)
  }

}
