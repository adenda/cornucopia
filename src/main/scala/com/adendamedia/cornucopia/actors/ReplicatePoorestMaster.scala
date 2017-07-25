package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.ConfigNew.ReplicatePoorestMasterConfig
import com.adendamedia.cornucopia.redis.{ClusterOperations, RedisHelpers}
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.pattern.pipe
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.redis.ClusterOperations.{ClusterConnectionsType, RedisUriToNodeId}
import com.adendamedia.cornucopia.redis.RedisHelpers.RedisClusterConnectionsInvalidException
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.lambdaworks.redis.RedisURI

import scala.concurrent.ExecutionContext

object ReplicatePoorestMasterSupervisor {
  def props(implicit config: ReplicatePoorestMasterConfig,
            clusterOperations: ClusterOperations): Props = Props(new ReplicatePoorestMasterSupervisor)

  val name = "replicatePoorestMasterConfig"
}

class ReplicatePoorestMasterSupervisor(implicit config: ReplicatePoorestMasterConfig,
                                       clusterOperations: ClusterOperations) extends CornucopiaSupervisor {
  import Overseer._

  override def receive: Receive = accepting

  private val findPoorestMaster: ActorRef = context.actorOf(FindPoorestMaster.props, FindPoorestMaster.name)

  override def accepting: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
      findPoorestMaster ! msg
      context.become(processing(msg, sender))
  }

  override def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case msg: ReplicatedMaster =>
      log.info(s"Successfully replicated master with new slave: ${msg.newSlaveUri}")
      ref ! msg
      context.unbecome()
  }
}

object FindPoorestMaster {
  def props(implicit config: ReplicatePoorestMasterConfig,
            clusterOperations: ClusterOperations): Props = Props(new FindPoorestMaster)

  val name = "findPoorestMaster"
}

class FindPoorestMaster(implicit config: ReplicatePoorestMasterConfig,
                        clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import Overseer._

  val replicatePoorestMaster = context.actorOf(ReplicatePoorestMaster.props, ReplicatePoorestMaster.name)

  override def receive: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
      findPoorestMaster(msg, sender)
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception)
      throw e
  }

  private def findPoorestMaster(msg: ReplicatePoorestMasterUsingSlave, supervisor: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    val connections = msg.connections
    clusterOperations.findPoorestMaster(connections) map { poorestMaster =>
      ReplicateMaster(msg.slaveUri, poorestMaster, msg.connections, msg.redisUriToNodeId, supervisor)
    } recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo replicatePoorestMaster
  }

}

object ReplicatePoorestMaster {
  def props(implicit config: ReplicatePoorestMasterConfig,
            clusterOperations: ClusterOperations): Props = Props(new ReplicatePoorestMaster)

  val name = "replicatePoorestMaster"
}

class ReplicatePoorestMaster(implicit config: ReplicatePoorestMasterConfig,
                             clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import Overseer._

  override def receive: Receive = {
    case msg: ReplicateMaster =>
      replicateMaster(msg)
  }

  private def replicateMaster(msg: ReplicateMaster) = {
    val supervisor = msg.ref
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.replicateMaster(msg.slaveUri, msg.masterNodeId, msg.connections, msg.redisUriToNodeId) map { _ =>
      ReplicatedMaster(msg.slaveUri)
    } recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo supervisor
  }

}
