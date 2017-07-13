package com.adendamedia.cornucopia.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.actor.Status.{Success, Failure}
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.CornucopiaException._
import Overseer.OverseerCommand

import scala.concurrent.duration._
import scala.util.Try
import com.lambdaworks.redis.RedisURI

object JoinRedisNode {
  def props(delegateProps: Props): Props = Props(new JoinRedisNode(delegateProps))

  case class Passthrough(result: RedisURI)
  case class Fail(message: OverseerCommand)

  trait AddingNodeType
  case object AddingMasterNode extends AddingNodeType
  case object AddingSlaveNode extends AddingNodeType
}

class JoinRedisNode(delegateProps: Props) extends Actor with ActorLogging {
  import Overseer._
  import JoinRedisNode._

  override def preRestart(reason: Throwable,
                          message: Option[Any]): Unit = {
    log.info(s"preRestart. Reason: $reason when handling message: $message")

    message match {
      case Some(Fail(msg)) => self ! msg
      case _ => ;
    }

    super.preRestart(reason, message)
  }

  val delegate = context.system.actorOf(delegateProps)

  override def receive: Receive = accepting

  private def accepting: Receive = {
    case join: JoinMasterNode =>
      delegate ! join
      context.become(delegating(AddingMasterNode, sender))
  }

  private def delegating(nodeType: AddingNodeType, ref: ActorRef): Receive = {
    case Fail(message: OverseerCommand) =>
      log.error("fail")
      throw FailedOverseerCommand(message)
    case Passthrough(uri: RedisURI) =>
      log.info("passthrough")
      nodeType match {
        case AddingMasterNode =>
          ref ! MasterNodeAdded(uri)
          context.become(accepting)
        case AddingSlaveNode =>
          ref ! SlaveNodeAdded(uri)
          context.become(accepting)
      }
  }

}

object JoinRedisNodeDelegate {
  def props(implicit clusterOperations: ClusterOperations): Props = Props(new JoinRedisNodeDelegate)
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
      case _ =>
        log.error(s"Wat!")
    } pipeTo ref
  }

}
