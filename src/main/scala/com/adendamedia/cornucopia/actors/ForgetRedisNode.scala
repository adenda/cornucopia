package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import akka.pattern.pipe
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.Config.ForgetRedisNodeConfig
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

object ForgetRedisNodeSupervisor {
  def props(implicit config: ForgetRedisNodeConfig, clusterOperations: ClusterOperations) =
    Props(new ForgetRedisNodeSupervisor)

  val name = "forgetRedisNodeSupervisor"

  case object Retry
}

class ForgetRedisNodeSupervisor(implicit config: ForgetRedisNodeConfig, clusterOperations: ClusterOperations)
  extends CornucopiaSupervisor {

  import Overseer._
  import ClusterOperations._
  import ForgetRedisNodeSupervisor._

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: CornucopiaForgetNodeException =>
      log.error(s"Error forgetting node: ${e.message}")
      self ! Retry
      Restart
  }

  private val forgetRedisNode: ActorRef = context.actorOf(ForgetRedisNode.props, ForgetRedisNode.name)

  override def receive: Receive = accepting

  protected def accepting: Receive = {
    case cmd: ForgetNode =>
      log.info(s"Forgetting redis node: ${cmd.uri}")
      forgetRedisNode ! cmd
      context.become(processing(cmd, sender))
  }

  protected def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case event: NodeForgotten =>
      implicit val executionContext: ExecutionContext = config.executionContext
      context.system.scheduler.scheduleOnce(config.refreshTimeout.seconds) {
        log.info(s"Successfully forgot node ${event.uri}")
        ref ! event
        context.unbecome()
      }
    case Retry =>
      log.info(s"Retrying to forget redis node")
      forgetRedisNode ! command
  }
}

object ForgetRedisNode {
  def props(implicit config: ForgetRedisNodeConfig, clusterOperations: ClusterOperations) =
    Props(new ForgetRedisNode)

  val name = "forgetRedisNode"
}

class ForgetRedisNode(implicit config: ForgetRedisNodeConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  override def receive: Receive = {
    case cmd: ForgetNode => forgetNode(cmd, sender)
    case kill: KillChild =>
      val reason = kill.reason.getOrElse(new Exception)
      throw reason
  }

  private def forgetNode(cmd: ForgetNode, ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    val uri = cmd.uri
    clusterOperations.forgetNode(uri) map (_ => NodeForgotten(uri)) recover { case e =>
      self ! KillChild(cmd, Some(e))
    } pipeTo ref
  }

}
