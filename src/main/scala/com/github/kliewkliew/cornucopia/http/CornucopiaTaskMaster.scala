package com.github.kliewkliew.cornucopia.http

import akka.actor._
import akka.util.Timeout
import com.github.kliewkliew.cornucopia.actors._
import com.github.kliewkliew.cornucopia.graph
import com.github.kliewkliew.cornucopia.redis.Connection.{newSaladAPI, Salad}

object CornucopiaTaskMaster {
  def props(implicit timeout: Timeout) = Props(new CornucopiaTaskMaster)

  case class RestTask(operation: String, redisNodeIp: String)
}

class CornucopiaTaskMaster(implicit timeout: Timeout) extends Actor with ActorLogging {
  import CornucopiaTaskMaster._
  import CornucopiaSource._

  implicit val newSaladAPIimpl: Salad = newSaladAPI
  val ref: ActorRef = new graph.CornucopiaActorSource().ref

  def receive = {
    case RestTask(operation, redisNodeIp) =>
      log.info(s"Received Cornucopia API task request: '$operation', '$redisNodeIp'")
      sender ! Right("its all good")
      ref ! Task(operation, redisNodeIp, Some(self))
    case Right(msg) =>
      log.info(s"Received task completion: $msg")
    case Left(msg) =>
      log.info(s"Received task failed: $msg")
  }
}
