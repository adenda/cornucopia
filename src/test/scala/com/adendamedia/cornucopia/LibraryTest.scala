package com.adendamedia.cornucopia

import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.redis.Connection.Salad
import akka.testkit.{TestKit, TestProbe}
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._
import org.mockito.Matchers._
import akka.stream.scaladsl.Flow
import com.adendamedia.cornucopia.graph.CornucopiaActorSource

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class LibraryTest extends TestKit(ActorSystem("LibraryTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  trait FakeCornucopiaActorSourceGraph {

    // hostname IP address must be semantically correct, java.net actually checks for RFC conformance
    val redisHost = "192.168.0.1"
    val redisUri = s"redis://$redisHost"

    val fakeSalad = mock[Salad]
    when(fakeSalad.canonicalizeURI(anyObject())).thenReturn(RedisURI.create(redisUri))

    class CornucopiaActorSourceLocal extends CornucopiaActorSource {
      lazy val probe = TestProbe()

      override val redisCommandRouter = SharedTestGraph.graph.redisCommandRouter

      override def getNewSaladApi: Salad = fakeSalad

      override def streamRemoveNode(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", Some("")))

      override def streamRemoveSlave(implicit executionContext: ExecutionContext) =
        Flow[KeyValue].map(_ => KeyValue("test", Some("")))

      override protected def waitForTopologyRefresh[T](passthrough: T)
                                                      (implicit executionContext: ExecutionContext): Future[T] = Future {
        passthrough
      }

      override protected def waitForTopologyRefresh2[T, U](passthrough1: T, passthrough2: U)
                                                 (implicit executionContext: ExecutionContext): Future[(T, U)] = Future {
        (passthrough1, passthrough2)
      }

      override protected def logTopology(implicit executionContext: ExecutionContext): Future[Unit] = Future(Unit)

      protected def reshardCluster(withoutNodes: Seq[String]): Future[Unit] = Future(Unit)

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

  "Add nodes" must {
    "add new master node and reshard cluster, or new slave node" ignore new FakeCornucopiaActorSourceGraph {
      import Library.source._

      // TODO: Fix problem with non-unique router name when creating two separate Actor systems
      val cornucopiaActorSourceLocal = new CornucopiaActorSourceLocal

      private val ref = cornucopiaActorSourceLocal.ref

      implicit val timeout = Timeout(5 seconds)

      // new master
      val future1 = ask(ref, Task("+master", Some(redisUri)))

      future1.onComplete {
        case Failure(_) => assert(false)
        case Success(msg) =>
          assert(msg == Right("master", redisHost))
      }

      Await.ready(future1, timeout.duration)

      // new slave
      val future2 = ask(ref, Task("+slave", Some(redisUri)))

      future2.onComplete {
        case Failure(_) => assert(false)
        case Success(msg) =>
          assert(msg == Right("slave", redisHost))
      }

      Await.ready(future2, timeout.duration)
    }
  }

}
