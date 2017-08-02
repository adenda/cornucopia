package com.adendamedia.cornucopia.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, OneForOneStrategy}
import akka.pattern.pipe
import akka.actor.SupervisorStrategy.Restart
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.ConfigNew.ClusterTopologyConfig

import scala.concurrent.ExecutionContext

object ClusterTopologySupervisor {
  def props(implicit clusterOperations: ClusterOperations,
            config: ClusterTopologyConfig): Props = Props(new ClusterTopologySupervisor)

  val name = "clusterTopologySupervisor"
}

class ClusterTopologySupervisor(implicit clusterOperations: ClusterOperations,
                                config: ClusterTopologyConfig) extends CornucopiaSupervisor {
  import Overseer._

  private val clusterTopology = context.actorOf(ClusterTopology.props, ClusterTopology.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e =>
      log.error(s"Error logging cluster topology: {}", e)
      clusterTopology ! LogTopology
      Restart
  }

  override def receive: Receive = accepting

  protected def accepting: Receive = {
    case LogTopology =>
      clusterTopology ! LogTopology
      context.become(processing(LogTopology, sender))
  }

  protected def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case TopologyLogged =>
      ref ! TopologyLogged
      context.unbecome()
  }
}

object ClusterTopology {
  def props(implicit clusterOperations: ClusterOperations,
            config: ClusterTopologyConfig): Props = Props(new ClusterTopology)

  val name = "clusterTopology"
}

class ClusterTopology(implicit clusterOperations: ClusterOperations,
                      config: ClusterTopologyConfig) extends Actor with ActorLogging {
  import Overseer._

  override def receive: Receive = {
    case LogTopology =>
      logTopology(sender)
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception("An unknown error occurred"))
      throw e
  }

  private def logTopology(ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.getClusterTopology map { topology =>
      log.info(s"Master nodes: ${topology.getOrElse("masters", "")}")
      log.info(s"Slave nodes: ${topology.getOrElse("slaves", "")}")
      TopologyLogged
    } recover {
      case e => self ! KillChild(LogTopology, Some(e))
    } pipeTo ref
  }

}
