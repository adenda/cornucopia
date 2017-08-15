package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, Terminated}
import akka.pattern.pipe
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.CornucopiaException._
import Overseer.OverseerCommand
import com.adendamedia.cornucopia.Config.JoinRedisNodeConfig
import com.lambdaworks.redis.RedisURI

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import Overseer.JoinNode

object JoinRedisNodeSupervisor {
  def props(implicit config: JoinRedisNodeConfig, clusterOperations: ClusterOperations) =
    Props(new JoinRedisNodeSupervisor)

  val name = "joinRedisNodeSupervisor"

  case object Retry
}

/**
  * The supervisor actor for the join redis node action is used to signal a failed attempt to join a node by throwing an
  * exception.
  */
class JoinRedisNodeSupervisor[C <: JoinNode](implicit config: JoinRedisNodeConfig,
                              clusterOperations: ClusterOperations) extends CornucopiaSupervisor[JoinNode] {
  import Overseer._
  import JoinRedisNodeSupervisor._

  private val joinRedisNodeProps = JoinRedisNode.props
  private val joinRedisNode = context.actorOf(joinRedisNodeProps, JoinRedisNode.name)
  context.watch(joinRedisNode)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case _: FailedOverseerCommand =>
      implicit val executionContext: ExecutionContext = config.executionContext
      context.system.scheduler.scheduleOnce(config.retryBackoffTime.seconds) {
        self ! Retry
      }
      Restart
  }

  def receive: Receive = accepting

  override def accepting: Receive = {
    case join: JoinNode =>
      joinRedisNode ! join
      context.become(processing[JoinNode](join, sender))
  }

  override def processing[D <: JoinNode](command: D, ref: ActorRef): Receive = {
    case evt: NodeJoinedEvent =>
      ref ! evt
      context.unbecome()
    case Retry =>
      log.info(s"Retrying to join redis node ${command.redisURI}")
      joinRedisNode ! command
    case Terminated(_) =>
      context.unbecome()
      throw FailedAddingRedisNodeException(s"Could not join Redis node to cluster after ${config.maxNrRetries} retries")
  }
}

object JoinRedisNode {
  def props(implicit clusterOperations: ClusterOperations, config: JoinRedisNodeConfig): Props =
    Props(new JoinRedisNode)

  val name = "joinRedisNode"

  case class Passthrough(result: RedisURI)
  case class Fail(message: OverseerCommand)

  trait AddingNodeType
  case object AddingMasterNode extends AddingNodeType
  case object AddingSlaveNode extends AddingNodeType
}

class JoinRedisNode(implicit clusterOperations: ClusterOperations, config: JoinRedisNodeConfig)
  extends Actor with ActorLogging {

  import Overseer._
  import JoinRedisNode._

  private val delegateProps = JoinRedisNodeDelegate.props
  private val delegate = context.actorOf(delegateProps, JoinRedisNodeDelegate.name)

  override def receive: Receive = accepting

  private def accepting: Receive = {
    case joinMaster: JoinMasterNode =>
      val ref = sender
      delegate ! joinMaster
      context.become(delegating(AddingMasterNode, ref))
    case joinSlave: JoinSlaveNode =>
      val ref = sender
      delegate ! joinSlave
      context.become(delegating(AddingSlaveNode, ref))
  }

  private def delegating(nodeType: AddingNodeType, ref: ActorRef): Receive = {
    case Fail(message: OverseerCommand) =>
      throw FailedOverseerCommand(message)
    case Passthrough(uri: RedisURI) =>
      log.info("Node joined")
      nodeType match {
        case AddingMasterNode =>
          implicit val executionContext: ExecutionContext = config.executionContext
          context.system.scheduler.scheduleOnce(config.refreshTimeout.seconds) {
            ref ! MasterNodeJoined(uri)
            context.become(accepting)
          }
        case AddingSlaveNode =>
          implicit val executionContext: ExecutionContext = config.executionContext
          context.system.scheduler.scheduleOnce(config.refreshTimeout.seconds) {
            ref ! SlaveNodeJoined(uri)
            context.become(accepting)
          }
      }
  }

}

object JoinRedisNodeDelegate {
  def props(implicit clusterOperations: ClusterOperations): Props = Props(new JoinRedisNodeDelegate)

  val name = "joinRedisNodeDelegate"
}

/**
  * Delegate to perform the actual adding of the node to the redis cluster. If the adding of a node to the cluster is a
  * success, then send `Passthrough` message to sender. If the adding of a node fails due to a Redis connection error,
  * then send a `Fail` message to sender
  * @param clusterOperations The functions used to send Redis commands to the cluster
  */
class JoinRedisNodeDelegate(implicit clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import Overseer._
  import JoinRedisNode._
  import ClusterOperations._
  import context.dispatcher

  override def receive: Receive = {
    case join: JoinNode => joinNode(join, sender)
  }

  /**
    * Attempts to add a node to the cluster
    * @param join The message containing the redis URI of the node to join to the cluster
    * @param ref The sender to pipe the message back to
    * @return Passthrough(uri) if success, or Fail(join) if fail sent back to the sender
    */
  private def joinNode(join: JoinNode, ref: ActorRef) = {
    clusterOperations.addNodeToCluster(join.redisURI) map {
      uri => Passthrough(uri)
    } recover {
      case e: CornucopiaRedisConnectionException =>
        log.error(s"Failed to join node ${join.redisURI.toURI} with error: ${e.message}")
        Fail(join)
    } pipeTo ref
  }

}
