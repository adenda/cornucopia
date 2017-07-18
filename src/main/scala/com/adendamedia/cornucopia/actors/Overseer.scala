package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ReshardTableNew.ReshardTableType
import com.adendamedia.cornucopia.Config
import com.lambdaworks.redis.RedisURI

import scala.concurrent.duration._

object Overseer {
  def props(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
            reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
            clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
            clusterReadySupervisorMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations): Props =
    Props(new Overseer(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, clusterConnectionsSupervisorMaker,
      clusterReadySupervisorMaker)
    )

  trait OverseerCommand

  trait NodeAddedEvent {
    val uri: RedisURI
  }

  trait JoinNode extends OverseerCommand {
    val redisURI: RedisURI
  }

  case class JoinMasterNode(redisURI: RedisURI) extends JoinNode
  case class JoinSlaveNode(redisURI: RedisURI) extends JoinNode

  case class MasterNodeAdded(uri: RedisURI) extends NodeAddedEvent
  case class SlaveNodeAdded(uri: RedisURI) extends NodeAddedEvent

  trait NodeJoinedEvent {
    val uri: RedisURI
  }
  case class MasterNodeJoined(uri: RedisURI) extends NodeJoinedEvent
  case class SlaveNodeJoined(uri: RedisURI) extends NodeJoinedEvent

  trait Reshard
  case class ReshardWithNewMaster(uri: RedisURI) extends Reshard

  case object GetClusterConnections extends OverseerCommand
  case class GotClusterConnections(connections: ClusterOperations.ClusterConnectionsType)

  case class GotReshardTable(reshardTable: ReshardTableType)

  case class KillChild(command: OverseerCommand)

  case class WaitForClusterToBeReady(connections: ClusterOperations.ClusterConnectionsType) extends OverseerCommand
  case object ClusterIsReady
  case object ClusterNotReady
}

/**
  * The overseer subscribes to Redis commands that have been published by the dispatcher. This actor is the parent
  * actor of all actors that process Redis cluster commands. Cluster commands include adding and removing nodes. The
  * overseer subscribes to the Shutdown message, whereby after receiving this message, it will restart its children.
  */
class Overseer(joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef,
               reshardClusterSupervisorMaker: ActorRefFactory => ActorRef,
               clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef,
               clusterReadySupervisorMaker: ActorRefFactory => ActorRef)
              (implicit clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import MessageBus.{AddNode, AddMaster, AddSlave, Shutdown, FailedAddingMasterRedisNode}
  import Overseer._
  import ClusterOperations.ClusterConnectionsType

  import context.dispatcher

  val joinRedisNodeSupervisor: ActorRef = joinRedisNodeSupervisorMaker(context)
  val reshardClusterSupervisor: ActorRef = reshardClusterSupervisorMaker(context)
  val clusterConnectionsSupervisor: ActorRef = clusterConnectionsSupervisorMaker(context)
  val clusterReadySupervisor: ActorRef = clusterReadySupervisorMaker(context)

  context.system.eventStream.subscribe(self, classOf[AddNode])
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
      log.debug(s"Received message AddMaster(${m.uri})")
      joinRedisNodeSupervisor ! JoinMasterNode(m.uri)
      context.become(joiningNode(m.uri))
    case s: AddSlave =>
      log.debug(s"Received message AddSlave(${s.uri})")
      joinRedisNodeSupervisor ! JoinSlaveNode(s.uri)
      context.become(joiningNode(s.uri))
//    case masterNodeAdded: MasterNodeAdded =>
//      log.info(s"Successfully added master Redis node ${masterNodeAdded.uri} to cluster")
//      context.system.eventStream.publish(MasterNodeAdded(masterNodeAdded.uri))
//    case slaveNodeAdded: SlaveNodeAdded =>
//      log.info(s"Successfully added slave Redis node ${slaveNodeAdded.uri} to cluster")
//      context.system.eventStream.publish(SlaveNodeAdded(slaveNodeAdded.uri))
  }

  private def joiningNode(uri: RedisURI): Receive = {
    case masterNodeJoined: MasterNodeJoined =>
      log.info(s"Master Redis node ${masterNodeJoined.uri} successfully joined")
      reshardClusterSupervisor ! ReshardWithNewMaster(uri)
      clusterConnectionsSupervisor ! GetClusterConnections
      context.become(resharding(uri))
    case slaveNodeJoined: SlaveNodeJoined =>
      log.info(s"Slave Redis node ${slaveNodeJoined.uri} successfully joined")
      // TODO: replicate poorest master
  }

  private def resharding(uri: RedisURI, reshardTable: Option[ReshardTableType] = None,
                         clusterConnections: Option[ClusterConnectionsType] = None): Receive = {
    case reshard: ReshardWithNewMaster =>
      log.info(s"ReshardWithNewMaster $uri") // TODO: I can't remember why this should be here
    case GotClusterConnections(connections) =>
      log.info(s"Got cluster connections")
      reshardTable match {
        case Some(table) =>
          clusterReadySupervisor ! WaitForClusterToBeReady(connections)
          context.become(waitingForClusterToBeReady(uri, table, connections))
        case None =>
          context.become(resharding(uri, reshardTable, Some(connections)))
      }
    case GotReshardTable(table) =>
      log.info(s"Got rehard table")
      clusterConnections match {
        case Some(connections) =>
          clusterReadySupervisor ! WaitForClusterToBeReady(connections)
          context.become(waitingForClusterToBeReady(uri, table, connections))
        case None =>
          context.become(resharding(uri, Some(table), clusterConnections))
      }
  }

  private def waitingForClusterToBeReady(uri: RedisURI, reshardTable: ReshardTableType,
                                         clusterConnections: ClusterConnectionsType): Receive = {
    case ClusterReady => // TODO: next step
  }

}
