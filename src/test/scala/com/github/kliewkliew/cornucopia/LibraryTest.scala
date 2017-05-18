package com.github.kliewkliew.cornucopia

import com.github.kliewkliew.cornucopia.redis._
import com.lambdaworks.redis.RedisURI
//import com.github.kliewkliew.cornucopia._
import com.github.kliewkliew.cornucopia.graph._
import com.github.kliewkliew.cornucopia.redis.Connection.Salad
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.actor.{ActorSystem, ActorRef}
import akka.pattern.ask
//import akka.actor.Status.Failure
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar._
import org.mockito.Matchers._
import com.github.kliewkliew.cornucopia.graph._
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{ Success, Failure }

class LibraryTest extends TestKit(ActorSystem("LibraryTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  trait FakeCornucopiaActorSourceGraph {
    import Config.Consumer.materializer

    // hostname IP address must be semantically correct, java.net actually checks for RFC conformance
    val redisUri = "redis://192.168.0.1"

    val fakeSalad = mock[Salad]
    when(fakeSalad.canonicalizeURI(anyObject())).thenReturn(RedisURI.create(redisUri))

    class CornucopiaActorSourceLocal extends CornucopiaActorSource {
      lazy val probe = TestProbe()

      override def getNewSaladApi: Salad = fakeSalad

      override def streamAddSlave(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override def streamRemoveNode(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override def streamRemoveSlave(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", ""))

      override protected def waitForTopologyRefresh[T](passthrough: T)
                                                      (implicit executionContext: ExecutionContext): Future[T] = Future {
        passthrough
      }

      override protected def waitForTopologyRefresh2[T, U](passthrough1: T, passthrough2: U)
                                                 (implicit executionContext: ExecutionContext): Future[(T, U)] = Future {
        (passthrough1, passthrough2)
      }

      override protected def logTopology(implicit executionContext: ExecutionContext): Future[Unit] = Future(Unit)

      override protected def reshardCluster(withoutNodes: Seq[String]): Future[Unit] = Future(Unit)

      override protected def addNodesToCluster(redisURIList: Seq[RedisURI], retries: Int = 0)
                                              (implicit executionContext: ExecutionContext): Future[Seq[RedisURI]] = {
        Future(redisURIList)
      }

      override protected def findMasters(redisURIList: Seq[RedisURI])
                                        (implicit executionContext: ExecutionContext): Future[Unit] = Future(Unit)

      override protected def reshardClusterWithNewMaster(newMasterURI: RedisURI): Future[Unit] = Future(Unit)

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

      implicit val timeout = Timeout(5 seconds)

      val future = ask(ref, Task("+master", redisUri))

      future.onComplete {
        case Failure(_) => assert(false)
        case Success(msg) =>
          assert(msg == Right("master"))
      }

      Await.ready(future, timeout.duration)
    }
  }

  "Add slave" must {
    "add new slave and find masters" in new FakeCornucopiaActorSourceGraph {
      import Library.source._

      val cornucopiaActorSourceLocal = new CornucopiaActorSourceLocal

      private val ref = cornucopiaActorSourceLocal.ref

      implicit val timeout = Timeout(5 seconds)

      val future = ask(ref, Task("+slave", redisUri))

      future.onComplete {
        case Failure(_) => assert(false)
        case Success(msg) =>
          assert(msg == Right("slave"))
      }

      Await.ready(future, timeout.duration)
    }
  }

}
