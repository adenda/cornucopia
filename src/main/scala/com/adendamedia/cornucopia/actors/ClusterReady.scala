package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.ConfigNew.ClusterReadyConfig
import com.adendamedia.cornucopia.redis.ClusterOperations
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.actor.SupervisorStrategy.Restart
import akka.pattern.pipe

import scala.concurrent.duration._
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.redis.ClusterOperations.ClusterConnectionsType

object ClusterReadySupervisor {
  def props(implicit config: ClusterReadyConfig, clusterOperations: ClusterOperations): Props =
    Props(new ClusterReadySupervisor)

  val name = "nodeReadySupervisor"
}

class ClusterReadySupervisor(implicit config: ClusterReadyConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  val clusterReady = context.actorOf(ClusterReady.props, ClusterReady.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: FailedOverseerCommand =>
      log.error("Error waiting for node to become ready, retrying")
      clusterReady ! e.overseerCommand
      Restart
  }

  override def receive: Receive = accepting

  private def accepting: Receive = {
    case wait: WaitForClusterToBeReady =>
      clusterReady ! wait
      context.become(waitingForClusterToBeReady(sender, wait))
  }

  private def waitingForClusterToBeReady(ref: ActorRef, wait: WaitForClusterToBeReady): Receive = {
    case ClusterIsReady =>
      ref forward ClusterIsReady
      context.become(accepting)
    case ClusterNotReady =>
      val delay: Int = config.backOffTime
      log.warning(s"Cluster not yet ready, checking again in $delay seconds")
      implicit val executionContext = config.executionContext
      context.system.scheduler.scheduleOnce(delay.seconds) {
        clusterReady ! wait
      }
  }
}

object ClusterReady {
  def props(implicit config: ClusterReadyConfig, clusterOperations: ClusterOperations): Props =
    Props(new ClusterReady)

  val name = "clusterReady"
}

class ClusterReady(implicit config: ClusterReadyConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  override def receive: Receive = {
    case wait: WaitForClusterToBeReady => waitForClusterToBeReady(wait, sender)
    case KillChild(cmd) => throw FailedOverseerCommand(cmd)
  }

  private def waitForClusterToBeReady(ready: WaitForClusterToBeReady, ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    clusterOperations.isClusterReady(ready.connections) map {
      case true => ClusterIsReady
      case false =>
        log.error(s"Cluster is not ready!")
        throw FailedOverseerCommand(ready)
    } recover {
      case e => self ! KillChild(ready)
    } pipeTo ref
  }

}

