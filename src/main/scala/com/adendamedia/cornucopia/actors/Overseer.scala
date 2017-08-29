package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props}
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection.SaladAPI
import com.adendamedia.cornucopia.redis.ReshardTable.{ReshardTableType, Slot}
import com.adendamedia.cornucopia.redis.ClusterOperations.{ClusterConnectionsType, NodeIdToRedisUri, RedisUriToNodeId}
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

object Overseer {
  def props(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
            reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
            clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
            clusterReadySupervisorMaker: ActorRefFactory => ActorRef,
            migrateSlotSupervisorMaker: ActorRefFactory => ActorRef,
            replicatePoorestMasterSupervisorMaker: ActorRefFactory => ActorRef,
            failoverSupervisorMaker: ActorRefFactory => ActorRef,
            getSlavesOfMasterSupervisorMaker: ActorRefFactory => ActorRef,
            forgetRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
            clusterTopologyMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations): Props =
    Props(new Overseer(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, clusterConnectionsSupervisorMaker,
      clusterReadySupervisorMaker, migrateSlotSupervisorMaker, replicatePoorestMasterSupervisorMaker,
      failoverSupervisorMaker, getSlavesOfMasterSupervisorMaker, forgetRedisNodeSupervisorMaker,
      clusterTopologyMaker)
    )

  val name = "overseer"

  trait OverseerCommand

  trait RedisNodeCommand extends OverseerCommand {
    val uri: RedisURI
  }

  trait JoinNode extends RedisNodeCommand

  case class JoinMasterNode(uri: RedisURI) extends JoinNode
  case class JoinSlaveNode(uri: RedisURI) extends JoinNode

  trait NodeJoinedEvent {
    val uri: RedisURI
  }
  case class MasterNodeJoined(uri: RedisURI) extends NodeJoinedEvent
  case class SlaveNodeJoined(uri: RedisURI) extends NodeJoinedEvent

  trait Reshard extends OverseerCommand
  case class ReshardWithNewMaster(uri: RedisURI) extends Reshard
  case class ReshardWithoutRetiredMaster(uri: RedisURI) extends Reshard

  case class GetClusterConnections(newRedisUri: RedisURI) extends OverseerCommand
  case class GotClusterConnections(connections: (ClusterOperations.ClusterConnectionsType,ClusterOperations.RedisUriToNodeId, SaladAPI))

  case class GotReshardTable(reshardTable: ReshardTableType)

  case class KillChild(command: OverseerCommand, reason: Option[Throwable] = None)

  case class WaitForClusterToBeReady(connections: ClusterOperations.ClusterConnectionsType, uri: RedisURI) extends RedisNodeCommand
  case object ClusterIsReady
  case object ClusterNotReady

  trait MigrateSlotsCommand extends OverseerCommand

  case class MigrateSlotsForNewMaster(newMasterUri: RedisURI, connections: ClusterOperations.ClusterConnectionsType,
                                      redisUriToNodeId: RedisUriToNodeId, salad: SaladAPI,
                                      reshardTable: ReshardTableType) extends MigrateSlotsCommand

  case class MigrateSlotsWithoutRetiredMaster(retiredMasterUri: RedisURI, connections: ClusterOperations.ClusterConnectionsType,
                                              redisUriToNodeId: RedisUriToNodeId,
                                              nodeIdToRedisUri: NodeIdToRedisUri,
                                              salad: SaladAPI,
                                              reshardTable: ReshardTableType) extends MigrateSlotsCommand

  case class MigrateSlotJob(sourceNodeId: ClusterOperations.NodeId, targetNodeId: ClusterOperations.NodeId, slot: Slot,
                            connections: ClusterOperations.ClusterConnectionsType,
                            redisURI: Option[RedisURI] = None) extends OverseerCommand

  case class JobCompleted(job: OverseerCommand)

  case object Reset extends OverseerCommand

  case class ValidateConnections(msg: GetClusterConnections, connections: (ClusterConnectionsType, RedisUriToNodeId, SaladAPI)) extends OverseerCommand
  case object ClusterConnectionsValid
  case object ClusterConnectionsInvalid

  trait ReplicatePoorestMasterCommand extends OverseerCommand
  case class ReplicatePoorestMasterUsingSlave(slaveUri: RedisURI) extends ReplicatePoorestMasterCommand
  case class ReplicatePoorestRemainingMasterUsingSlave(slaveUri: RedisURI,
                                                       excludedMasters: List[RedisURI])
    extends ReplicatePoorestMasterCommand

  case class ReplicateMaster(slaveUri: RedisURI, masterNodeId: ClusterOperations.NodeId, ref: ActorRef)
    extends OverseerCommand
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

  case object LogTopology extends OverseerCommand
  case object TopologyLogged
}

/**
  * The overseer subscribes to Redis commands that have been published by the dispatcher. This actor is the parent
  * actor of all actors that process Redis cluster commands. Cluster commands include adding and removing nodes.
  */
class Overseer(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
               reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
               clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
               clusterReadySupervisorMaker: ActorRefFactory => ActorRef,
               migrateSlotSupervisorMaker: ActorRefFactory => ActorRef,
               replicatePoorestMasterSupervisorMaker: ActorRefFactory => ActorRef,
               failoverSupervisorMaker: ActorRefFactory => ActorRef,
               getSlavesOfMasterSupervisorMaker: ActorRefFactory => ActorRef,
               forgetRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
               clusterTopologyMaker: ActorRefFactory => ActorRef)
              (implicit clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import MessageBus._
  import Overseer._
  import ClusterOperations.ClusterConnectionsType

  val joinRedisNodeSupervisor: ActorRef = joinRedisNodeSupervisorMaker(context)
  val reshardClusterSupervisor: ActorRef = reshardClusterSupervisorMaker(context)
  val clusterConnectionsSupervisor: ActorRef = clusterConnectionsSupervisorMaker(context)
  val clusterReadySupervisor: ActorRef = clusterReadySupervisorMaker(context)
  val migrateSlotsSupervisor: ActorRef = migrateSlotSupervisorMaker(context)
  val replicatePoorestMasterSupervisor: ActorRef = replicatePoorestMasterSupervisorMaker(context)
  val failoverSupervisor: ActorRef = failoverSupervisorMaker(context)
  val getSlavesOfMasterSupervisor: ActorRef = getSlavesOfMasterSupervisorMaker(context)
  val forgetRedisNodeSupervisor: ActorRef = forgetRedisNodeSupervisorMaker(context)
  val clusterTopology: ActorRef = clusterTopologyMaker(context)

  context.system.eventStream.subscribe(self, classOf[AddNode])
  context.system.eventStream.subscribe(self, classOf[RemoveNode])
  context.system.eventStream.subscribe(self, classOf[Shutdown])

  log.info(s"I'm alive!")

  override def supervisorStrategy = OneForOneStrategy() {
    case FailedAddingRedisNodeException(message: String, command: RedisNodeCommand) =>
      log.error(s"$message: Restarting child actor")
      val uri = command.uri
      context.system.eventStream.publish(FailedAddingMasterRedisNode(reason = message, uri))
      Restart
    case FailedOverseerCommand(message: String, command: MigrateSlotsCommand, _) =>
      log.error(s"Overseer failed migrating slots while trying to process command $command")
      command match {
        case cmd: MigrateSlotsForNewMaster =>
          val uri = cmd.newMasterUri
          context.system.eventStream.publish(FailedAddingMasterRedisNode(message, uri))
        case cmd: MigrateSlotsWithoutRetiredMaster =>
          val uri = cmd.retiredMasterUri
          context.system.eventStream.publish(FailedRemovingMasterRedisNode(message, uri))
        case _ =>
          log.warning(s"Problem processing command $command")
      }
      context.unbecome()
      context.become(acceptingCommands)
      Restart
  }

  override def receive: Receive = acceptingCommands

  private def acceptingCommands: Receive = {
    case m: AddMaster =>
      log.info(s"Received message AddMaster(${m.uri})")
      joinRedisNodeSupervisor ! JoinMasterNode(m.uri)
      context.unbecome()
      context.become(joiningNode(m.uri))
    case s: AddSlave =>
      log.info(s"Received message AddSlave(${s.uri})")
      joinRedisNodeSupervisor ! JoinSlaveNode(s.uri)
      context.unbecome()
      context.become(joiningNode(s.uri))
    case rm: RemoveMaster =>
      log.info(s"Received message RemoveMaster(${rm.uri})")
      val msg = FailoverMaster(rm.uri)
      failoverSupervisor ! msg
      context.unbecome()
      context.become(failingOverForRemovingMaster(rm.uri))
    case rs: RemoveSlave =>
      log.info(s"Receive message RemoveSlave(${rs.uri})")
      val msg = FailoverSlave(rs.uri)
      failoverSupervisor ! msg
      context.unbecome()
      context.become(failingOverForRemovingSlave(rs.uri))
  }

  private def failingOverForRemovingMaster(uri: RedisURI): Receive = {
    case FailoverComplete =>
      log.info(s"Failover completed successfully, $uri is now a master node")
      clusterTopology ! LogTopology
    case TopologyLogged =>
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      reshardClusterSupervisor ! ReshardWithoutRetiredMaster(uri)
      context.unbecome()
      context.become(computingReshardTableForRemovingMaster(uri))
  }

  private def failingOverForRemovingSlave(uri: RedisURI): Receive = {
    case FailoverComplete =>
      log.info(s"Failover completed successfully, $uri is now a slave node")
      clusterTopology ! LogTopology
    case TopologyLogged =>
      val cmd = ForgetNode(uri)
      forgetRedisNodeSupervisor ! cmd
      context.unbecome()
      context.become(forgettingSlaveNode(cmd, uri))
  }

  private def computingReshardTableForRemovingMaster(retiredMaster: RedisURI,
                                                     clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId, SaladAPI)] = None,
                                                     reshardTable: Option[ReshardTableType] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections for removing retired master $retiredMaster")
      reshardTable match {
        case Some(table) =>
          // invert the uri to nodeId map
          val redisUriToNodeId: RedisUriToNodeId = connections._2
          val nodeIdToRedisUri: NodeIdToRedisUri = redisUriToNodeId.map(_.swap)
          val cmd = MigrateSlotsWithoutRetiredMaster(retiredMaster, connections._1, redisUriToNodeId, nodeIdToRedisUri, connections._3, table)
          migrateSlotsSupervisor ! cmd
          context.unbecome()
          context.become(migratingSlotsWithoutRetiredMaster(cmd))
        case None =>
          context.unbecome()
          context.become(computingReshardTableForRemovingMaster(retiredMaster, Some(connections), None))
      }
    case GotReshardTable(table) =>
      log.info(s"Got reshard table for removing retired master $retiredMaster")
      clusterConnections match {
        case Some(connections) =>
          val redisUriToNodeId: RedisUriToNodeId = connections._2
          val nodeIdToRedisUri: NodeIdToRedisUri = redisUriToNodeId.map(_.swap)
          val cmd = MigrateSlotsWithoutRetiredMaster(retiredMaster, connections._1, redisUriToNodeId, nodeIdToRedisUri, connections._3, table)
          migrateSlotsSupervisor ! cmd
          context.unbecome()
          context.become(migratingSlotsWithoutRetiredMaster(cmd))
        case None =>
          context.unbecome()
          context.become(computingReshardTableForRemovingMaster(retiredMaster, None, Some(table)))
      }
  }

  private def joiningNode(uri: RedisURI): Receive = {
    case masterNodeJoined: MasterNodeJoined =>
      log.info(s"Master Redis node ${masterNodeJoined.uri} successfully joined")
      reshardClusterSupervisor ! ReshardWithNewMaster(uri)
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      context.unbecome()
      context.become(reshardingWithNewMaster(uri))
    case slaveNodeJoined: SlaveNodeJoined =>
      log.info(s"Slave Redis node ${slaveNodeJoined.uri} successfully joined")
      clusterConnectionsSupervisor ! GetClusterConnections(uri)
      context.unbecome()
      context.become(addingSlaveNode(slaveNodeJoined.uri))
  }

  private def addingSlaveNode(uri: RedisURI,
                              clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId, SaladAPI)] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections")
      replicatePoorestMasterSupervisor ! ReplicatePoorestMasterUsingSlave(uri)
      context.unbecome()
      context.become(addingSlaveNode(uri, Some(connections)))
    case ReplicatedMaster(slaveUri) =>
      log.info(s"Successfully replicated master node by new slave node $uri")
      clusterConnections map {
        case (_, _, salad) => salad.shutdown()
        Unit
      }
      context.system.eventStream.publish(SlaveNodeAdded(slaveUri))
      context.unbecome()
      context.become(acceptingCommands)
  }

  /**
    * Computing the reshard table and getting the cluster connections is done concurrently
    * @param uri The Redis URI of the new master being added
    * @param reshardTable The compute reshard table
    * @param clusterConnections The connections to the master nodes
    */
  private def reshardingWithNewMaster(uri: RedisURI, reshardTable: Option[ReshardTableType] = None,
                                      clusterConnections: Option[(ClusterConnectionsType, RedisUriToNodeId, SaladAPI)] = None): Receive = {
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections")
      reshardTable match {
        case Some(table) =>
          if (connectionsAreValidForAddingNewMaster(table, connections, uri)) {
            clusterReadySupervisor ! WaitForClusterToBeReady(connections._1, uri)
            context.unbecome()
            context.become(waitingForClusterToBeReadyForNewMaster(uri, table, connections))
          }
          else {
            log.warning(s"Redis connections are not valid")
            clusterConnectionsSupervisor ! GetClusterConnections(uri)
            context.unbecome()
            context.become(reshardingWithNewMaster(uri, Some(table), None)) // discard invalid connections
          }
        case None =>
          context.unbecome()
          context.become(reshardingWithNewMaster(uri, None, Some(connections)))
      }
    case GotReshardTable(table) =>
      log.info(s"Got reshard table")
      clusterConnections match {
        case Some(connections) =>
          if (connectionsAreValidForAddingNewMaster(table, connections, uri)) {
            clusterReadySupervisor ! WaitForClusterToBeReady(connections._1, uri)
            context.unbecome()
            context.become(waitingForClusterToBeReadyForNewMaster(uri, table, connections))
          }
          else {
            log.warning(s"Redis connections are not valid")
            clusterConnectionsSupervisor ! GetClusterConnections(uri)
            context.unbecome()
            context.become(reshardingWithNewMaster(uri, Some(table), None)) // discard invalid connections
          }
        case None =>
          context.unbecome()
          context.become(reshardingWithNewMaster(uri, Some(table), None))
      }
  }

  private def connectionsAreValidForAddingNewMaster(table: ReshardTableType,
                                                    connections: (ClusterConnectionsType, RedisUriToNodeId, SaladAPI),
                                                    newRedisUri: RedisURI) = {
    // Since we validated the number of slots in the reshard table, we can be sure that it has all the master NodeId's in it
    val numNodes = table.keys.count(_ => true)
    val numConnections = connections._1.keys.count(_ != connections._2(newRedisUri))
    numNodes == numConnections
  }

  private def waitingForClusterToBeReadyForNewMaster(uri: RedisURI, reshardTable: ReshardTableType,
                                                     connections: (ClusterConnectionsType, RedisUriToNodeId, SaladAPI)): Receive = {
    case ClusterIsReady =>
      log.info(s"Cluster is ready, migrating slots")
      val msg = MigrateSlotsForNewMaster(uri, connections._1, connections._2, connections._3, reshardTable)
      migrateSlotsSupervisor ! msg
      context.unbecome()
      context.become(migratingSlotsForNewMaster(msg))
  }

  private def migratingSlotsForNewMaster(migrating: MigrateSlotsForNewMaster): Receive = {
    case JobCompleted(job: MigrateSlotsForNewMaster) =>
      log.info(s"Successfully added master node ${job.newMasterUri.toURI}")
      migrating.salad.shutdown()
      context.system.eventStream.publish(MasterNodeAdded(job.newMasterUri))
      context.unbecome()
      context.become(acceptingCommands)
  }

  private def migratingSlotsWithoutRetiredMaster(migrating: MigrateSlotsWithoutRetiredMaster): Receive = {
    case JobCompleted(job: MigrateSlotsWithoutRetiredMaster) =>
      log.info(s"Successfully resharded without retired master node ${job.retiredMasterUri.toURI}")
      val cmd = GetSlavesOf(job.retiredMasterUri)
      getSlavesOfMasterSupervisor ! cmd
      job.salad.shutdown()
      context.unbecome()
      context.become(gettingSlavesOfMaster(job.retiredMasterUri, cmd, job.connections, job.redisUriToNodeId))
  }

  private def gettingSlavesOfMaster(retiredMasterUri: RedisURI, command: GetSlavesOf,
                                    connections: ClusterConnectionsType,
                                    redisUriToNodeId: RedisUriToNodeId): Receive = {
    case event: GotSlavesOf =>
      log.info(s"Got slaves of retired master $retiredMasterUri: ${event.slaves.map(_.getUri.toURI)}")

      if (event.slaves.isEmpty) {
        log.warning(s"Retired master does not appear to be replicated by any slaves")
        val cmd = ForgetNode(retiredMasterUri)
        forgetRedisNodeSupervisor ! cmd
        context.unbecome()
        context.become(forgettingMasterNode(cmd, retiredMasterUri))
      } else {
        val excludedMaster = List(retiredMasterUri)
        event.slaves foreach { slave =>
          val msg = ReplicatePoorestRemainingMasterUsingSlave(slave.getUri, excludedMaster)
          replicatePoorestMasterSupervisor ! msg
        }
        val slaves = event.slaves.map(_.getUri)
        context.unbecome()
        context.become(
          replicatingPoorestRemainingMaster(retiredMasterUri, slaves, excludedMaster, connections, redisUriToNodeId)
        )
      }
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
          val msg = ReplicatePoorestRemainingMasterUsingSlave(x, excludedMasters)
          replicatePoorestMasterSupervisor ! msg
          context.unbecome()
          context.become(
            replicatingPoorestRemainingMaster(retiredMasterUri, xs, excludedMasters, connections, redisUriToNodeId)
          )
        case Nil =>
          val cmd = ForgetNode(retiredMasterUri)
          forgetRedisNodeSupervisor ! cmd
          context.unbecome()
          context.become(forgettingMasterNode(cmd, retiredMasterUri))
      }
  }

  private def forgettingMasterNode(overseerCommand: OverseerCommand, uri: RedisURI): Receive = {
    case event: NodeForgotten =>
      log.info(s"Successfully forgot master node ${event.uri}")
      val msg = MasterNodeRemoved(uri)
      context.system.eventStream.publish(msg)
      context.unbecome()
      context.become(acceptingCommands)
  }

  private def forgettingSlaveNode(overseerCommand: OverseerCommand, uri: RedisURI): Receive = {
    case event: NodeForgotten =>
      log.info(s"Successfully forgot slave node ${event.uri}")
      val msg = SlaveNodeRemoved(uri)
      context.system.eventStream.publish(msg)
      context.unbecome()
      context.become(acceptingCommands)
  }

}
