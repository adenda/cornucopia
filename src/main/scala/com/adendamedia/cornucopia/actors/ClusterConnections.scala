package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.ConfigNew.ClusterConnectionsConfig
import com.adendamedia.cornucopia.redis.{ClusterOperations, RedisHelpers}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.pipe
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.redis.ClusterOperations.{ClusterConnectionsType, RedisUriToNodeId}
import com.adendamedia.cornucopia.redis.RedisHelpers.RedisClusterConnectionsInvalidException
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

import scala.util.Try

object ClusterConnectionsSupervisor {
  def props(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
            redisHelpers: RedisHelpers): Props =
    Props(new ClusterConnectionsSupervisor)

  val name = "clusterConnectionsSupervisor"
}

class ClusterConnectionsSupervisor(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                                   redisHelpers: RedisHelpers)
  extends Actor with ActorLogging {

  import Overseer._

  val clusterConnectionsProps = ClusterConnections.props(self)
  val clusterConnections = context.actorOf(clusterConnectionsProps, ClusterConnections.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case _: FailedOverseerCommand =>
      log.error("Error getting cluster connections, retrying")
      clusterConnections ! GetClusterConnections
      Restart
    case _: RedisClusterConnectionsInvalidException =>
      log.error("Error validating cluster connections, retrying")
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
      log.info(s"Got cluster connections")
      ref forward conns
      context.become(accepting)
  }

}

object ClusterConnections {
  def props(supervisor: ActorRef)(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                                  redisHelpers: RedisHelpers): Props =
    Props(new ClusterConnections(supervisor))

  val name = "clusterConnections"

  case object Kill
}

class ClusterConnections(supervisor: ActorRef)
                        (implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                         redisHelpers: RedisHelpers)
  extends Actor with ActorLogging {

  import ClusterConnections._
  import Overseer._

  val props = ValidateClusterConnections.props
  val validateClusterConnections: ActorRef = context.actorOf(props, ValidateClusterConnections.name)

  override def supervisorStrategy = OneForOneStrategy() {
    case _: RedisClusterConnectionsInvalidException => Escalate
  }

  override def receive: Receive = {
    case GetClusterConnections => getConnections(sender)
    case Kill => throw FailedOverseerCommand(GetClusterConnections)
    case msg: GotClusterConnections => supervisor forward msg
  }

  private def getConnections(ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    clusterOperations.getClusterConnections map { connections =>
      ValidateConnections(connections)
    } recover {
      case e =>
        self ! Kill
    } pipeTo validateClusterConnections
  }

}

object ValidateClusterConnections {
  def props(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
            redisHelpers: RedisHelpers): Props =
    Props(new ValidateClusterConnections)

  val name = "validateClusterConnections"
}

class ValidateClusterConnections(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                                 redisHelpers: RedisHelpers)
  extends Actor with ActorLogging {

  import ClusterConnections._
  import Overseer._
  import RedisHelpers._

  override def receive: Receive = {
    case v: ValidateConnections =>
      validateConnections(v, sender)
    case kill: KillChild =>
      throw kill.reason.get // I'm not super stoked about this syntax, but the reason should always be set so it should
                            // be OK-ish.
  }

  private def validateConnections(v: ValidateConnections, ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    implicit val expectedTotalNumberSlots: Int = config.expectedTotalNumberSlots
    clusterOperations.getRedisMasterNodes map { masterNodes =>
      if (redisHelpers.compareUsingSlotsCount(masterNodes, v.connections)) GotClusterConnections(v.connections)
    } recover {
      case e: RedisClusterConnectionsInvalidException =>
        self ! KillChild(v, Some(e))
    } pipeTo ref
  }

}
