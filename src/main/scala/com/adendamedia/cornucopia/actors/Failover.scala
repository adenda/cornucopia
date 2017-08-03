package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import akka.pattern.pipe
import akka.actor.Status.{Failure, Success}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ReshardTableNew}
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.ConfigNew.FailoverConfig
import Overseer._
import com.adendamedia.cornucopia.actors.FailoverSupervisor.DoFailover

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

/**
  * When removing a redis node, it is necessary to specify whether a master or slave node should be removed, and the URI
  * of the node that should be removed. In some cases, if it's requested to remove a master node at a given URI, the
  * redis cluster node at that given URI is not a master. The same is the case when removing a slave node. In those
  * cases, it is necessary to run a `FAILOVER` command on the slave node that should become the master node. Once the
  * failover is complete, the redis node at the given URI now has the desired role--master or slave, and it can then be
  * removed.
  */
object FailoverSupervisor {
  def props(implicit config: FailoverConfig, clusterOperations: ClusterOperations): Props =
    Props(new FailoverSupervisor)

  val name = "failover"

  case class DoFailover(msg: FailoverCommand, nodeRole: ClusterOperations.Role)
  case object Retry
}

class FailoverSupervisor(implicit config: FailoverConfig, clusterOperations: ClusterOperations)
  extends CornucopiaSupervisor {

  import FailoverSupervisor._
  import ClusterOperations._

  private val getRole = context.actorOf(GetRole.props(self), GetRole.name)

  override def receive: Receive = accepting

  protected def accepting: Receive = {
    case msg: FailoverMaster =>
      log.info(s"Failing over master ${msg.uri}")
      getRole ! msg
      context.become(processing(msg, sender))
    case msg: FailoverSlave =>
      log.info(s"Failing over slave ${msg.uri}")
      getRole ! msg
      context.become(processing(msg, sender))
  }

  protected def processing(command: OverseerCommand, ref: ActorRef): Receive = {
    case FailoverComplete =>
      implicit val executionContext: ExecutionContext = config.executionContext
      context.system.scheduler.scheduleOnce(config.refreshTimeout.seconds) {
        ref forward FailoverComplete
        context.unbecome()
      }
    case Retry => getRole ! command
  }

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: CornucopiaFailoverVerificationFailedException =>
      log.error(e.message, e)
      self ! Retry
      Restart
    case e: FailedOverseerCommand =>
      self ! Retry
      Restart
  }

}

object GetRole {
  def props(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations): Props =
    Props(new GetRole(supervisor))

  val name = "getRole"
}

class GetRole(supervisor: ActorRef)
             (implicit config: FailoverConfig, clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import Overseer._

  private val failover: ActorRef = context.actorOf(Failover.props(supervisor), Failover.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: FailedOverseerCommand =>
      failover ! e.overseerCommand
      Restart
  }

  override def receive: Receive = {
    case msg: FailoverCommand =>
      log.info(s"Getting node role for doing failover")
      getRole(msg, sender)
    case e: KillChild =>
      log.error(s"Failed getting role", e)
      throw e.reason.getOrElse(new Exception)
  }

  private def getRole(msg: FailoverCommand, ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.getRole(msg.uri) map(DoFailover(msg, _)) recover {
      case e => self ! KillChild(msg)
    } pipeTo failover
  }
}

object Failover {
  def props(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations): Props =
    Props(new Failover(supervisor))

  val name = "failover"

  case class VerifyFailoverCommand(command: FailoverCommand)
  case class VerificationFailed(command: VerifyFailoverCommand)
  case class VerificationSuccess(command: VerifyFailoverCommand)
}

class Failover(supervisor: ActorRef)
              (implicit config: FailoverConfig, clusterOperations: ClusterOperations) extends Actor with ActorLogging {
  import Overseer._
  import GetRole._
  import FailoverSupervisor.DoFailover
  import ClusterOperations.{Role, Master, Slave}

  private val failoverWorker: ActorRef =
    context.actorOf(FailoverWorker.props(supervisor), FailoverWorker.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: FailedOverseerCommand =>
      failoverWorker ! e.overseerCommand
      Restart
  }

  override def receive: Receive = {
    case DoFailover(msg: FailoverMaster, nodeRole: Role) => nodeRole match {
      case Master =>
        log.info(s"Not doing failover because the node ${msg.uri} is already a master")
        supervisor ! FailoverComplete
      case Slave =>
        log.info(s"Doing failover now")
        failoverWorker forward msg
    }
    case DoFailover(msg: FailoverSlave, nodeRole: Role) => nodeRole match {
      case Master =>
        failoverWorker forward msg
      case Slave =>
        log.info(s"Not doing failover because the node ${msg.uri} is already a slave")
        supervisor ! FailoverComplete
    }
  }

}

object FailoverWorker {
  def props(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations): Props =
    Props(new FailoverWorker(supervisor))

  val name = "failoverWorker"
}

class FailoverWorker(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Failover._
  import ClusterOperations._
  import FailoverSupervisor._

  private val verifyFailover: ActorRef = context.actorOf(VerifyFailover.props(supervisor), VerifyFailover.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case e: FailedOverseerCommand =>
      verifyFailover ! e.overseerCommand
      Restart
  }

  override def receive: Receive = {
    case msg: FailoverMaster =>
      failoverMaster(msg)
      context.become(failingOver(0))
    case msg: FailoverSlave =>
      failoverSlave(msg)
      context.become(failingOver(0))
  }

  private def failingOver(retries: Int): Receive = {
    case VerificationSuccess =>
      supervisor ! FailoverComplete
    case msg: VerificationFailed =>
      val cmd = msg.command
      val attempts = config.maxNrAttemptsToVerify
      if (retries >= attempts)
        throw CornucopiaFailoverVerificationFailedException(s"Failed verifying failover by exceeding maximum retry attempts of $attempts")
      scheduleVerifyFailover(cmd)
      context.become(failingOver(retries + 1))
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception("An unknown error occurred"))
      log.error(s"Failed performing failover: {}", e)
      throw FailedOverseerCommand(kill.command)
  }

  private def scheduleVerifyFailover(cmd: VerifyFailoverCommand) = {
    val delay: Int = config.verificationRetryBackOffTime
    log.warning(s"Failover incomplete, checking again in $delay seconds")
    implicit val executionContext = config.executionContext
    context.system.scheduler.scheduleOnce(delay.seconds) {
      verifyFailover ! cmd
    }
  }

  private def failoverMaster(msg: FailoverMaster) = {
    log.info(s"Failing over master now")
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.failoverMaster(msg.uri) map (_ => VerifyFailoverCommand(msg)) recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo verifyFailover
  }

  private def failoverSlave(msg: FailoverSlave) = {
    log.info(s"Failing over slave now")
    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.failoverSlave(msg.uri) map (_ => VerifyFailoverCommand(msg)) recover {
      case e => self ! KillChild(msg, Some(e))
    } pipeTo verifyFailover
  }

}

object VerifyFailover {
  def props(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations): Props =
    Props(new VerifyFailover(supervisor))

  val name = "verifyFailover"
}

class VerifyFailover(supervisor: ActorRef)(implicit config: FailoverConfig, clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Failover._
  import VerifyFailover._
  import Overseer._
  import ClusterOperations.{Role, Master, Slave}

  override def receive: Receive = {
    case msg: VerifyFailoverCommand =>
      msg.command match {
        case FailoverMaster(_) =>
          log.info(s"Successfully failed over master, waiting for it to propagate")
          verify(msg, Master, sender)
        case FailoverSlave(_) =>
          log.info(s"Successfully failed over slave, waiting for it to propagate")
          verify(msg, Slave, sender)
      }
    case kill: KillChild =>
      val e = kill.reason.getOrElse(new Exception)
      log.error(s"Failed verifying failover", e)
      throw FailedOverseerCommand(kill.command)
  }

  private def verify(msg: VerifyFailoverCommand, role: Role, ref: ActorRef) = {
    implicit val executionContext: ExecutionContext = config.executionContext
    val uri = msg.command.uri
    clusterOperations.verifyFailover(uri, role) map {
      case true =>
        log.info(s"Failover completed")
        VerificationSuccess
      case false => VerificationFailed(msg)
    } recover {
      case e => self ! KillChild(msg.command, Some(e))
    } pipeTo ref
  }
}


