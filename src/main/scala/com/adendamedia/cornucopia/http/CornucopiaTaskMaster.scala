package com.adendamedia.cornucopia.http

import akka.actor._
import akka.util.Timeout
import com.adendamedia.cornucopia.actors.Gatekeeper
import com.adendamedia.cornucopia.redis.Connection.{Salad, newSaladAPI}

object CornucopiaTaskMaster {
  def props(implicit timeout: Timeout) = Props(new CornucopiaTaskMaster)

  case class RestTask(operation: String)
  case class RestTask2(operation: String, redisNodeIp: String)
}

class CornucopiaTaskMaster(implicit timeout: Timeout) extends Actor with ActorLogging {
  import CornucopiaTaskMaster._
  import com.adendamedia.cornucopia.actors.Gatekeeper._

  implicit val newSaladAPIimpl: Salad = newSaladAPI
  val ref: ActorRef = context.actorOf(Gatekeeper.props)

  def receive = {
    case RestTask(operation) =>
      log.error(s"Not allowed: $operation")
      sender ! Left("Operation not allowed")
    case RestTask2(operation, redisNodeIp) =>
      log.info(s"Received Cornucopia API task request: '$operation', '$redisNodeIp'")
      sender ! Right(s"Operation accepted: $operation, $redisNodeIp")
      // TODO: handle accepted/denied messages from Gatekeeper
      ref ! Task(operation, redisNodeIp)
  }
}
