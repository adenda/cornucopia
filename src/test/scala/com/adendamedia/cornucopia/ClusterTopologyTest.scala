package com.adendamedia.cornucopia

import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{ Success, Failure }
import redis.Connection.{newSaladAPI, Salad}

class ClusterTopologyTest extends TestKit(ActorSystem("ClusterTopologyTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  trait ClusterTopologyDebug {
    val redisUri = "redis://127.0.0.1"
    implicit val newSaladAPIimpl: Salad = newSaladAPI
  }

  implicit val ec = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Debugging" must {
    "be fun" in new ClusterTopologyDebug {
//      import Library.source._

//      val cornucopiaActorSource = new CornucopiaActorSource
//      val cornucopiaActorSource = SharedTestGraph.graph
//
//      private val ref = cornucopiaActorSource.ref
//
//      implicit val timeout = Timeout(20 seconds)
//
//      val future = ask(ref, Task("?topology"))
//
//      future.onComplete {
//        case Failure(_) => assert(false)
//        case Success(msg) =>
//          assert(msg == Right("master", redisUri))
//      }
//
//      Await.ready(future, timeout.duration)
    }
  }
}
