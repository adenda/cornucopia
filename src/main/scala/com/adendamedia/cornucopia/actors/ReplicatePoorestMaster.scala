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

object ReplicatePoorestMasterSupervisor {
  def props(implicit config: ReplicatePoorestMasterConfig): Props = Props(new ReplicatePoorestMasterSupervisor)

  val name = "replicatePoorestMasterConfig"
}

class ReplicatePoorestMasterSupervisor(implicit config: ReplicatePoorestMasterConfig) extends CornucopiaSupervisor {
  import Overseer._

  override def receive: Receive = accepting

  override def accepting: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
  }

  override def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case _ =>
  }
}

object FindPoorestMaster {
  def props(implicit config: ReplicatePoorestMasterConfig): Props = Props(new FindPoorestMaster)

  val name = "findPoorestMaster"

  case class ReplicateMaster(slaveUri: RedisURI, masterUri: RedisURI)
}

class FindPoorestMaster(implicit config: ReplicatePoorestMasterConfig) extends Actor with ActorLogging {
  import Overseer._

  override def receive: Receive = {
    case msg: ReplicatePoorestMasterUsingSlave =>
  }
}

object ReplicatePoorestMaster {
  def props(implicit config: ReplicatePoorestMasterConfig): Props = Props(new ReplicatePoorestMaster)

  val name = "replicatePoorestMaster"
}

class ReplicatePoorestMaster(implicit config: ReplicatePoorestMasterConfig) extends Actor with ActorLogging {
  import Overseer._
  import FindPoorestMaster._

  override def receive: Receive = {
    case msg: ReplicateMaster =>
  }
}
