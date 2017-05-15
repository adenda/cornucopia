package com.github.kliewkliew.cornucopia.http

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._

class RestApi(system: ActorSystem, timeout: Timeout) extends RestRoutes {
  implicit val requestTimeout = timeout
  implicit def executionContext = system.dispatcher

  def createCornucopiaTaskMaster = system.actorOf(CornucopiaTaskMaster.props)
}

trait RestRoutes extends CornucopiaApi with EventMarshalling {
  import StatusCodes._
  import CornucopiaTaskMaster._

  def routes: Route = taskRoute

  def taskRoute = pathPrefix("task") {
    pathEndOrSingleSlash {
      post {
        entity(as[RestTask]) { ed =>
          onSuccess(submitTask(ed.operation, ed.redisNodeIp)) {
            case Left(msg) =>
              complete(BadRequest, msg)
            case Right(msg) =>
              complete(Accepted, msg)
          }
        }
      }
    }
  }
}

trait CornucopiaApi {
  import CornucopiaTaskMaster._

  def createCornucopiaTaskMaster(): ActorRef

  implicit def executionContext: ExecutionContext
  implicit def requestTimeout: Timeout

  lazy val cornucopiaTaskMaster = createCornucopiaTaskMaster()

  def submitTask(operation: String, redisNodeIp: String) = {
    cornucopiaTaskMaster.ask(RestTask(operation, redisNodeIp)).mapTo[Either[String, String]]
  }
}

