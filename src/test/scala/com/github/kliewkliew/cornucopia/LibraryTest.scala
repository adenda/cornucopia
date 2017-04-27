package com.github.kliewkliew.cornucopia

//import com.github.kliewkliew.cornucopia._
import com.github.kliewkliew.cornucopia.graph._
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.actor.{ActorSystem, ActorRef}
import akka.actor.Status.Failure
import akka.stream.ActorMaterializer
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar._
import com.github.kliewkliew.cornucopia.graph._
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class LibraryTest extends TestKit(ActorSystem("LibraryTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  trait FakeCornucopiaActorSourceGraph {
    import com.github.kliewkliew.cornucopia.kafka.Config.Consumer.materializer

    class CornucopiaActorSourceLocal extends CornucopiaActorSource {
      lazy val probe = TestProbe()

      override def streamAddMaster(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(kv => {
          val ip = kv.value
          probe.ref ! "streamAddMaster"
          Thread.sleep(300)
          probe.ref ! ip
          Thread.sleep(300)
          KeyValue("*reshard", "")
        })

      override def streamAddSlave(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override def streamRemoveNode(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override def streamRemoveSlave(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override def streamReshard(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => {
          probe.ref ! "streamReshard"
          KeyValue("*reshard", "")
        })
    }

  }

  implicit val ec = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Add master" must {
    "add new master and reshard cluster" in new FakeCornucopiaActorSourceGraph {
      import Library.source._

      val cornucopiaActorSourceLocal = new CornucopiaActorSourceLocal

      private val ref = cornucopiaActorSourceLocal.ref

      private val redisUri = "redis://123.456.789.10"

      ref ! Task("+master", redisUri)

      cornucopiaActorSourceLocal.probe.expectMsg(100 millis, "streamAddMaster")

      cornucopiaActorSourceLocal.probe.expectMsg(350 millis, redisUri)

      cornucopiaActorSourceLocal.probe.expectMsg(700 millis, "streamReshard")
    }
  }

}
