package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.Config.ClusterConnectionsConfig
import com.adendamedia.cornucopia.redis.{ClusterOperations, RedisHelpers}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.pipe
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.redis.RedisHelpers.RedisClusterConnectionsInvalidException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object ClusterConnectionsSupervisor {
  def props(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
            redisHelpers: RedisHelpers): Props =
    Props(new ClusterConnectionsSupervisor)

  val name = "clusterConnectionsSupervisor"

  case object Retry
}

class ClusterConnectionsSupervisor(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                                   redisHelpers: RedisHelpers)
  extends Actor with ActorLogging {

  import Overseer._
  import ClusterConnectionsSupervisor._

  val clusterConnectionsProps = ClusterConnections.props(self)
  val clusterConnections = context.actorOf(clusterConnectionsProps, ClusterConnections.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case _: FailedOverseerCommand =>
      implicit val executionContext: ExecutionContext = config.executionContext
      log.error("Error getting cluster connections, retrying")
      context.system.scheduler.scheduleOnce(2.seconds)(self ! Retry)
      Restart
    case _: RedisClusterConnectionsInvalidException =>
      log.error("Error validating cluster connections, retrying")
      implicit val executionContext: ExecutionContext = config.executionContext
      context.system.scheduler.scheduleOnce(2.seconds)(self ! Retry)
      Restart
  }

  override def receive: Receive = accepting

  private def accepting: Receive = {
    case get: GetClusterConnections =>
      clusterConnections ! get
      context.become(gettingClusterConnections(get, sender))
  }

  private def gettingClusterConnections(get: GetClusterConnections, ref: ActorRef): Receive = {
    case conns: GotClusterConnections =>
      log.info(s"Got cluster connections")
      ref forward conns
      context.become(accepting)
    case Retry =>
      clusterConnections ! get
  }

}

object ClusterConnections {
  def props(supervisor: ActorRef)(implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                                  redisHelpers: RedisHelpers): Props =
    Props(new ClusterConnections(supervisor))

  val name = "clusterConnections"
}

class ClusterConnections(supervisor: ActorRef)
                        (implicit config: ClusterConnectionsConfig, clusterOperations: ClusterOperations,
                         redisHelpers: RedisHelpers)
  extends Actor with ActorLogging {

  import Overseer._

  val props = ValidateClusterConnections.props
  val validateClusterConnections: ActorRef = context.actorOf(props, ValidateClusterConnections.name)

  override def supervisorStrategy = OneForOneStrategy() {
    case _: RedisClusterConnectionsInvalidException => Escalate
  }

  override def receive: Receive = {
    case get: GetClusterConnections => getConnections(get, sender)
    case kill: KillChild => throw FailedOverseerCommand(kill.command)
    case msg: GotClusterConnections => supervisor forward msg
  }

  private def getConnections(msg: GetClusterConnections, ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    clusterOperations.getClusterConnections map { connections =>
      ValidateConnections(msg, connections)
    } recover {
      case e =>
        self ! KillChild(msg)
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

  import Overseer._
  import RedisHelpers._

  override def receive: Receive = {
    case v: ValidateConnections =>
      validateConnections(v, sender)
    case kill: KillChild =>
      throw kill.reason.getOrElse(new Exception("An unknown error occurred"))
  }

  private def validateConnections(v: ValidateConnections, ref: ActorRef) = {
    implicit val executionContext = config.executionContext
    implicit val expectedTotalNumberSlots: Int = config.expectedTotalNumberSlots
    val newRedisURI = v.msg.newRedisUri // newly added redis node is a master without slot assignments

    val connectionsToVerify = (v.connections._1, v.connections._2)

    clusterOperations.getRedisMasterNodes map { masterNodes =>
      // TODO: This is ugly, and probably bad b/c it uses exceptions for flow control. Maybe there is a better way.
      if (redisHelpers.compareUsingSlotsCount(masterNodes, connectionsToVerify) &&
          redisHelpers.connectionsHaveRedisNode(newRedisURI, connectionsToVerify)) GotClusterConnections(v.connections)
    } recover {
      case e: RedisClusterConnectionsInvalidException =>
        self ! KillChild(v, Some(e))
    } pipeTo ref
  }

}
