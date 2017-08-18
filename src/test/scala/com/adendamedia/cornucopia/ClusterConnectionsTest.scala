package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ClusterOperations._
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.RedisHelpers
import com.adendamedia.cornucopia.redis.ReshardTable
import com.adendamedia.cornucopia.Config.ClusterConnectionsConfig
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar
import ClusterConnectionsTest._

class ClusterConnectionsTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import Overseer._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    implicit object ClusterConnectionsConfigTest extends ClusterConnectionsConfig {
      val maxNrRetries: Int = 2
      val executionContext: ExecutionContext = system.dispatcher
      override val expectedTotalNumberSlots: Int = 42 // don't care
      override val retryBackoffTime: Int = 0
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
    implicit val redisHelpers: RedisHelpers = mock[RedisHelpers]

    val saladApiDummy: Connection.SaladAPI = mock[Connection.SaladAPI]

    val dummyConnections = (Map.empty[NodeId, Connection.Salad], Map.empty[RedisURI, NodeId], saladApiDummy)
    val dummies = (dummyConnections._1, dummyConnections._2)

    val dummyMasters: List[RedisClusterNode] = List.empty[RedisClusterNode]

    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
  }

  "ClusterConnections" must {
    "010 - retry if there is an error connecting to cluster nodes" in new TestConfig {

      implicit val executionContext: ExecutionContext = ClusterConnectionsConfigTest.executionContext

      when(clusterOperations.getClusterConnections).thenReturn(
        Future.failed(new Exception("foobar42"))
      )

      val props = ClusterConnectionsSupervisor.props
      val clusterConnectionsSupervisor = TestActorRef[ClusterConnectionsSupervisor](props)

      val expectedErrorMessage = "Error getting cluster connections, retrying"
      val msg = GetClusterConnections(redisURI)

      EventFilter.error(pattern = expectedErrorMessage,
        occurrences = ClusterConnectionsConfigTest.maxNrRetries + 1) intercept {
        clusterConnectionsSupervisor ! msg
      }
    }

    "020 - retry if there is an error validating the connections" in new TestConfig {
      import RedisHelpers._

      implicit val executionContext: ExecutionContext = ClusterConnectionsConfigTest.executionContext

      when(clusterOperations.getClusterConnections).thenReturn(
        Future.successful(dummyConnections)
      )

      when(clusterOperations.getRedisMasterNodes).thenReturn(
        Future.successful(dummyMasters)
      )


      when(redisHelpers.compareUsingSlotsCount(dummyMasters, dummies)(ClusterConnectionsConfigTest.expectedTotalNumberSlots)).thenThrow(
        RedisClusterConnectionsInvalidException("wat")
      )

      when(redisHelpers.connectionsHaveRedisNode(redisURI, dummies)).thenReturn(
        true
      )

      val props = ClusterConnectionsSupervisor.props
      val clusterConnectionsSupervisor = TestActorRef[ClusterConnectionsSupervisor](props)

      val expectedErrorMessage = "Error validating cluster connections, retrying"
      val msg = GetClusterConnections(redisURI)

      EventFilter.error(message = expectedErrorMessage,
        occurrences = ClusterConnectionsConfigTest.maxNrRetries + 1) intercept {
        clusterConnectionsSupervisor ! msg
      }
    }

    "030 - get the cluster connections" in new TestConfig {
      import ClusterOperations._
      import com.adendamedia.cornucopia.redis.Connection
      import RedisHelpers._

      implicit val executionContext: ExecutionContext = ClusterConnectionsConfigTest.executionContext

      when(clusterOperations.getClusterConnections).thenReturn(
        Future.successful(dummyConnections)
      )

      when(clusterOperations.getRedisMasterNodes).thenReturn(
        Future.successful(dummyMasters)
      )

      when(redisHelpers.compareUsingSlotsCount(dummyMasters, dummies)(ClusterConnectionsConfigTest.expectedTotalNumberSlots)).thenReturn(
        true
      )

      when(redisHelpers.connectionsHaveRedisNode(redisURI, dummies)).thenReturn(
        true
      )

      val props = ClusterConnectionsSupervisor.props
      val clusterConnectionsSupervisor = TestActorRef[ClusterConnectionsSupervisor](props)

      clusterConnectionsSupervisor ! GetClusterConnections(redisURI)

      expectMsg {
        GotClusterConnections(dummyConnections)
      }
    }
  }

}

object ClusterConnectionsTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("ClusterConnectionsTest", config)
  }
}


