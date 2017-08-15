package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.redis._
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import com.lambdaworks.redis.RedisURI

object Gatekeeper {
  def props: Props = Props(new Gatekeeper)

  final case class Task(operation: String, redisNodeIp: String)
  case object TaskAccepted
  case object TaskDenied

  val name = "gatekeeper"
}

/**
  * Processes raw Redis commands and forwards them to the Dispatcher if they are valid. If the commands are invalid,
  * sends a TaskDenied response back to client ActorRef.
  */
class Gatekeeper extends Actor with ActorLogging {
  import Gatekeeper._
  import Dispatcher._

  private val dispatcher: ActorRef = context.actorOf(Dispatcher.props, Dispatcher.name)

  def receive: Receive = {
    case Task(operation, redisNodeIp) => processTask(operation, redisNodeIp, sender)
  }

  private def processTask(operation: String, redisNodeIp: String, ref: ActorRef) = {
    chooseOperation(operation) match {
      case validOperation: Operation =>
        validateIp(validOperation, redisNodeIp, ref)
      case UNSUPPORTED =>
        log.error(s"Could not process submitted task: operation '$operation' is invalid.")
        ref ! TaskDenied
    }
  }

  private def validateIp(operation: Operation, redisNodeIp: String, ref: ActorRef) = {
    createRedisUri(redisNodeIp) match {
      case Success(redisUri: RedisURI) =>
        log.info(s"Task accepted: ${operation.message} ${redisUri.toURI}")
        dispatcher.tell(DispatchTask(operation, redisUri), ref)
      case Failure(_) =>
        log.error(s"Could not process submitted task: Redis node Ip '$redisNodeIp' is invalid.")
        ref ! TaskDenied
    }
  }

  private def chooseOperation(key: String): Operation = key.trim.toLowerCase match {
    case ADD_MASTER.key          => ADD_MASTER
    case ADD_SLAVE.key           => ADD_SLAVE
    case REMOVE_MASTER.key       => REMOVE_MASTER
    case REMOVE_SLAVE.key        => REMOVE_SLAVE
    case RESHARD.key             => RESHARD
    case CLUSTER_TOPOLOGY.key    => CLUSTER_TOPOLOGY
    case _                       => UNSUPPORTED
  }

  /**
    * Create Redis URI from the following forms: host OR host:port
    * e.g., redis://127.0.0.1 OR redis://127.0.0.1:7006
    */
  private def createRedisUri(uri: String): Try[RedisURI] = {
    Try {
      val parts = uri.split(":")
      if (parts.size == 3) {
        val host = parts(1).foldLeft("")((acc, ch) => if (ch != '/') acc + ch else acc)
        RedisURI.create(host, parts(2).toInt)
      }
      else RedisURI.create(uri)
    }
  }

}
