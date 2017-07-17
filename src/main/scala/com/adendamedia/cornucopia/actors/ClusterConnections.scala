package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.ConfigNew.ClusterConnectionsConfig
import com.adendamedia.cornucopia.redis.ClusterOperations
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.actor.SupervisorStrategy.Restart
import akka.pattern.pipe
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand

object ClusterConnectionsSupervisor {
  def props(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations): Props =
    Props(new ClusterConnectionsSupervisor)

  val name = "clusterConnectionsSupervisor"
}

class ClusterConnectionsSupervisor(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  val clusterConnectionsProps = ClusterConnections.props
  val clusterConnections = context.actorOf(clusterConnectionsProps)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case _: FailedOverseerCommand =>
      log.error("Error getting cluster connections, retrying")
      clusterConnections ! GetClusterConnections
      Restart
  }

  override def receive: Receive = accepting

  private def accepting: Receive = {
    case GetClusterConnections =>
      clusterConnections ! GetClusterConnections
      context.become(gettingClusterConnections(sender))
  }

  private def gettingClusterConnections(ref: ActorRef): Receive = {
    case conns: GotClusterConnections =>
      ref forward conns
      context.become(accepting)
  }

}

object ClusterConnections {
  def props(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations): Props =
    Props(new ClusterConnections)

  val name = "clusterConnections"

  case object KillMyself
}

class ClusterConnections(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import ClusterConnections._
  import Overseer._

  override def receive: Receive = {
    case GetClusterConnections => getConnections(sender)
    case KillMyself => throw FailedOverseerCommand(GetClusterConnections)
  }

  private def getConnections(ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    clusterOperations.getClusterConnections map { connections =>
      GotClusterConnections(connections)
    } recover {
      case e => self ! KillMyself
    } pipeTo ref
  }

}
