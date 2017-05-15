package com.github.kliewkliew.cornucopia.http

import akka.actor._
import akka.util.Timeout

object CornucopiaTaskMaster {
  def props(implicit timeout: Timeout) = Props(new CornucopiaTaskMaster)

  case class RestTask(operation: String, redisNodeIp: String)
}

class CornucopiaTaskMaster(implicit timeout: Timeout) extends Actor with ActorLogging {
  import CornucopiaTaskMaster._
  import context._

  def receive = {
    case RestTask(operation, redisNodeIp) =>
      log.info(s"Received Cornucopia API task request: '$operation', '$redisNodeIp'")
      sender ! Right("its all good")
      // TODO: send it down the graph
  }
}
