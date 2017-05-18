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
import redis.Connection.{newSaladAPI, Salad}
import redis.ReshardTable
import redis.ReshardTable._
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

class ReshardTest extends TestKit(ActorSystem("ReshardTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  trait ReshardTableTest {
    val one: java.util.List[Integer] = new java.util.ArrayList[Integer](java.util.Arrays.asList[Integer](1,2,3,4,5,6))
    val two: java.util.List[Integer] = new java.util.ArrayList[Integer](java.util.Arrays.asList[Integer](7,8,9,10,11,12))
    val three: java.util.List[Integer] = new java.util.ArrayList[Integer](java.util.Arrays.asList[Integer](13,14,15,16,17))

    class RedisClusterNodeTest(private val slots: java.util.List[Integer], private val nodeId: String) extends RedisClusterNode {
      override def getSlots: java.util.List[Integer] = slots
      override def getNodeId: String = nodeId
    }

    val node1 = new RedisClusterNodeTest(one, "a")
    val node2 = new RedisClusterNodeTest(two, "b")
    val node3 = new RedisClusterNodeTest(three, "c")

    val clusterNodes = List(node1, node2, node3)

    val expectedReshardTable: ReshardTableType = Map(
      "a" -> List(1,2),
      "b" -> List(7),
      "c" -> List(13)
    )

  }

  trait ReshardDebug {
    val redisUri = "redis://127.0.0.1"
    implicit val newSaladAPIimpl: Salad = newSaladAPI
  }

  implicit val ec = system.dispatcher

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Reshard cluster with new master" must {
    "calculate reshard table correctly" in new ReshardTableTest {

      final implicit val ExpectedTotalNumberSlots: Int = 17
      val reshardTable: ReshardTableType = computeReshardTable(clusterNodes)

      assert(reshardTable == expectedReshardTable)
    }
  }

  "Reshard cluster with new master" must {
    "throw a ReshardTableException if the expected total number of slots does not match the actual number of slots" in new ReshardTableTest {

      final implicit val ExpectedTotalNumberSlots: Int = 1234

      try {
        val reshardTable: ReshardTableType = computeReshardTable(clusterNodes)
        assert(false)
      } catch {
        case e: ReshardTableException => assert(true)
        case _ => assert(false)
      }

    }
  }

  "Debugging" must {
    "be fun" ignore new ReshardDebug {
      import Library.source._

      val cornucopiaActorSource = new CornucopiaActorSource

      private val ref = cornucopiaActorSource.ref

      implicit val timeout = Timeout(6 seconds)

      val future = ask(ref, Task("+master", redisUri))

      future.onComplete {
        case Failure(_) => assert(false)
        case Success(msg) =>
          assert(msg == Right("master"))
      }

      Await.ready(future, timeout.duration)
    }
  }

}

