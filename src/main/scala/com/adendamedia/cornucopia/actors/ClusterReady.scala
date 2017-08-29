package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.Config.ClusterReadyConfig
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.CornucopiaException._
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, Terminated}
import akka.actor.SupervisorStrategy.Restart
import akka.pattern.pipe

import scala.concurrent.duration._
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.actors.Overseer.WaitForClusterToBeReady

import scala.concurrent.ExecutionContext

object ClusterReadySupervisor {
  def props(implicit config: ClusterReadyConfig, clusterOperations: ClusterOperations): Props =
    Props(new ClusterReadySupervisor)

  val name = "clusterReadySupervisor"
}

class ClusterReadySupervisor[C <: WaitForClusterToBeReady](implicit config: ClusterReadyConfig,
                                                           clusterOperations: ClusterOperations)
  extends CornucopiaSupervisor[WaitForClusterToBeReady] {

  import Overseer._

  private val clusterReady = context.actorOf(ClusterReady.props, ClusterReady.name)

  context.watch(clusterReady)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: FailedOverseerCommand =>
      log.error("Error waiting for node to become ready, retrying")
      scheduleReadinessCheck(e.overseerCommand)
      Restart
  }

  override def receive: Receive = accepting

  override def accepting: Receive = {
    case wait: WaitForClusterToBeReady =>
      clusterReady ! wait
      context.become(processing(wait, sender))
  }

  private def scheduleReadinessCheck(cmd: OverseerCommand) = {
    val delay: Int = config.backOffTime
    log.warning(s"Cluster not yet ready, checking again in $delay seconds")
    implicit val executionContext: ExecutionContext = config.executionContext
    context.system.scheduler.scheduleOnce(delay.seconds) {
      clusterReady ! cmd
    }
  }

  protected def processing[D <: WaitForClusterToBeReady](command: D, ref: ActorRef): Receive = {
    case ClusterIsReady =>
      ref forward ClusterIsReady
      context.unbecome()
    case ClusterNotReady =>
      scheduleReadinessCheck(command)
    case Terminated(_) =>
      context.unbecome()
      throw FailedAddingRedisNodeException(s"Cluster was not ready after ${config.maxNrRetries} retries", command)
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
    case wait: WaitForClusterToBeReady =>
      context.become(waitingForClusterToBeReady(0))
      waitForClusterToBeReady(wait, sender)
    case KillChild(cmd, _) => throw FailedOverseerCommand(overseerCommand = cmd)
  }

  def waitingForClusterToBeReady(retries: Int): Receive = {
    case wait: WaitForClusterToBeReady =>
      val maxRetries = config.clusterReadyRetries
      if (retries > maxRetries) {
        context.unbecome()
        throw FailedOverseerCommand(message = s"Failed waiting for cluster to be ready after maximum retries of $maxRetries reached", overseerCommand = wait)
      } else {
        context.unbecome()
        context.become(waitingForClusterToBeReady(retries + 1))
        waitForClusterToBeReady(wait, sender)
      }
    case KillChild(cmd, _) =>
      context.unbecome()
      throw FailedOverseerCommand(overseerCommand = cmd)
  }

  private def waitForClusterToBeReady(ready: WaitForClusterToBeReady, ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.isClusterReady(ready.connections) map {
      case true =>
        context.unbecome()
        ClusterIsReady
      case false =>
        ClusterNotReady
    } recover {
      case e =>
        log.error(s"Failed waiting for cluster to be ready: {}", e)
        self ! KillChild(ready)
    } pipeTo ref
  }

}

