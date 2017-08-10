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
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.adendamedia.cornucopia.redis.ReshardTableNewImpl._

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

    val clusterNodesPrime = List(node1, node2)

    val expectedReshardTablePrime: ReshardTableType = Map(
      "a" -> List(13,14,15),
      "b" -> List(16,17)
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

    "throw a ReshardTableException if the expected total number of slots does not match the actual number of slots" in new ReshardTableTest {

      final implicit val ExpectedTotalNumberSlots: Int = 1234

      try {
        val reshardTable: ReshardTableType = computeReshardTable(clusterNodes)
        assert(false)
      } catch {
        case e: ReshardTableException => assert(true)
        case _: Throwable => assert(false)
      }

    }
  }

  "Reshard cluster without retired master" must {
    "calculate reshard table correctly" in new ReshardTableTest {

      final implicit val ExpectedTotalNumberSlots: Int = 17
      val reshardTable: ReshardTableType = computeReshardTablePrime(node3, clusterNodesPrime)

      assert(reshardTable == expectedReshardTablePrime)
    }

  }



//  "Debugging" must {
//    "be fun" ignore new ReshardDebug {
//      import Library.source._
//
////      val cornucopiaActorSource = new CornucopiaActorSource
//      val cornucopiaActorSource = SharedTestGraph.graph
//
//      private val ref = cornucopiaActorSource.ref
//
//      implicit val timeout = Timeout(6 seconds)
//
//      val future = ask(ref, Task("+master", Some(redisUri)))
//
//      future.onComplete {
//        case Failure(_) => assert(false)
//        case Success(msg) =>
//          assert(msg == Right("master", redisUri))
//      }
//
//      Await.ready(future, timeout.duration)
//    }
//  }

}

