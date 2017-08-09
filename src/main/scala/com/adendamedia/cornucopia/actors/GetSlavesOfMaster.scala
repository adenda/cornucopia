package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.ConfigNew.GetSlavesOfMasterConfig
import com.adendamedia.cornucopia.redis.ClusterOperations
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import com.lambdaworks.redis.RedisURI
import akka.actor.SupervisorStrategy.Restart
import akka.pattern.pipe

import scala.concurrent.duration._
import com.adendamedia.cornucopia.CornucopiaException.FailedOverseerCommand
import com.adendamedia.cornucopia.redis.ClusterOperations.ClusterConnectionsType

import scala.concurrent.ExecutionContext

object GetSlavesOfMasterSupervisor {
  def props(implicit config: GetSlavesOfMasterConfig, clusterOperations: ClusterOperations): Props =
    Props(new GetSlavesOfMasterSupervisor)

  val name = "getSlavesOfMasterSupervisor"

  case object Retry
}

class GetSlavesOfMasterSupervisor(implicit config: GetSlavesOfMasterConfig, clusterOperations: ClusterOperations)
  extends CornucopiaSupervisor {

  import Overseer._
  import GetSlavesOfMasterSupervisor._

  private val props: Props = GetSlavesOfMaster.props
  private val getSlavesOfMaster: ActorRef = context.actorOf(props, GetSlavesOfMaster.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e =>
      log.error("Error retrieving slaves of master, retrying", e)
      self ! Retry
      Restart
  }

  override def receive: Receive = accepting

  protected def accepting: Receive = {
    case msg: GetSlavesOf =>
      log.info(s"Getting the slaves of master nodes ${msg.masterUri}")
      getSlavesOfMaster ! msg
      context.become(processing(msg, sender))
  }

  protected def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case Retry =>
      getSlavesOfMaster ! command
    case event: GotSlavesOf =>
      ref ! event
      context.unbecome()
  }
}

object GetSlavesOfMaster {
  def props(implicit config: GetSlavesOfMasterConfig, clusterOperations: ClusterOperations): Props =
    Props(new GetSlavesOfMaster)

  val name = "getSlavesOfMaster"
}

class GetSlavesOfMaster(implicit config: GetSlavesOfMasterConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  override def receive: Receive = {
    case cmd: GetSlavesOf =>
      getSlavesOfMaster(cmd, sender)
    case kill: KillChild =>
      val cause = kill.reason.getOrElse(new Exception)
      throw cause
  }

  private def getSlavesOfMaster(command: Overseer.GetSlavesOf, ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    val uri = command.masterUri
    clusterOperations.getSlavesOfMaster(uri) map(GotSlavesOf(uri, _)) recover {
      case e => self ! KillChild(command, Some(e))
    } pipeTo ref
  }
}
