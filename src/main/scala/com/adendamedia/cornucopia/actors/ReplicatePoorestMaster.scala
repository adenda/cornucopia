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

  val name = "replicatePoorestMasterSupervisor"
}

class ReplicatePoorestMasterSupervisor(implicit config: ReplicatePoorestMasterConfig,
                                       clusterOperations: ClusterOperations) extends CornucopiaSupervisor {
  import Overseer._
  import ClusterOperations._

  override def receive: Receive = accepting

  private val findPoorestMaster: ActorRef = context.actorOf(FindPoorestMaster.props, FindPoorestMaster.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: CornucopiaFindPoorestMasterException =>
      log.error(s"Failed to find poorest master: {}", e)
      Restart
  }

  override def accepting: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
      log.info(s"Received message to replicate poorest master with redis node ${msg.slaveUri}")
      findPoorestMaster ! msg
      context.become(processing(msg, sender))
    case msg: ReplicatePoorestRemainingMasterUsingSlave =>
      log.info(s"Received message to replicate poorest remaining master with slave node ${msg.slaveUri}")
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

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    // Retry by sending the same message back to self
    self ! message.get
    super.preRestart(reason, message)
  }

  override def receive: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
      findPoorestMaster(msg, sender)
    case msg: ReplicatePoorestRemainingMasterUsingSlave =>
      findPoorestRemainingMaster(msg, sender)
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception)
      throw e
  }

  private def findPoorestRemainingMaster(msg: ReplicatePoorestRemainingMasterUsingSlave, supervisor: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    val excludedMasters = msg.excludedMasters
    clusterOperations.findPoorestRemainingMaster(excludedMasters) map { poorestMaster =>
      log.info(s"Found poorest remaining master to replicate: $poorestMaster")
      ReplicateMaster(msg.slaveUri, poorestMaster, supervisor)
    } recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo replicatePoorestMaster
  }

  private def findPoorestMaster(msg: ReplicatePoorestMasterUsingSlave, supervisor: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.findPoorestMaster map { poorestMaster =>
      log.info(s"Found poorest master to replicate: $poorestMaster")
      ReplicateMaster(msg.slaveUri, poorestMaster, supervisor)
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
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception)
      throw e
  }

  private def replicateMaster(msg: ReplicateMaster) = {
    val supervisor = msg.ref
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.replicateMaster(msg.slaveUri, msg.masterNodeId) map { _ =>
      ReplicatedMaster(msg.slaveUri)
    } recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo supervisor
  }

}
