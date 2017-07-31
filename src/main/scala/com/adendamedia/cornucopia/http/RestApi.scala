package com.adendamedia.cornucopia.http

import scala.concurrent.ExecutionContext

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

  def createCornucopiaTaskMaster = system.actorOf(CornucopiaTaskMaster.props, CornucopiaTaskMaster.name)
}

trait RestRoutes extends CornucopiaApi with EventMarshalling {
  import StatusCodes._
  import CornucopiaTaskMaster._

  def routes: Route = taskRoute ~ taskRoute2

  def taskRoute = pathPrefix("task") {
    pathEndOrSingleSlash {
      post { // POST /task
        entity(as[RestTask]) { ed =>
          onSuccess(submitTask(ed.operation)) {
            case Left(msg) =>
              complete(BadRequest, msg)
            case Right(msg) =>
              complete(Accepted, msg)
          }
        }
      }
    }
  }

  def taskRoute2 = pathPrefix("task2") {
    pathEndOrSingleSlash {
      post { // POST /task2
        entity(as[RestTask2]) { ed =>
          onSuccess(submitTask2(ed.operation, ed.redisNodeIp)) {
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

  def submitTask(operation: String) = {
    cornucopiaTaskMaster.ask(RestTask(operation)).mapTo[Either[String, String]]
  }

  def submitTask2(operation: String, redisNodeIp: String) = {
    cornucopiaTaskMaster.ask(RestTask2(operation, redisNodeIp)).mapTo[Either[String, String]]
  }
}

