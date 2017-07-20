package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ReshardTableNew
import com.adendamedia.cornucopia.ConfigNew.ClusterReadyConfig
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar

import ClusterReadyTest._

class ClusterReadyTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import Overseer._
  import ClusterOperations._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    implicit object ClusterReadyConfigTest extends ClusterReadyConfig {
      val maxNrRetries: Int = 2
      val executionContext: ExecutionContext = system.dispatcher
      val backOffTime: Int = 1
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
  }

  "ClusterReady" must {
    "retry if there is an exception" in new TestConfig {

      implicit val executionContext: ExecutionContext = ClusterReadyConfigTest.executionContext

      val dummyConnections = Map.empty[NodeId, Connection.Salad]

      when(clusterOperations.isClusterReady(dummyConnections)).thenReturn(
        Future.failed(new Exception)
      )

      val props = ClusterReadySupervisor.props
      val clusterReadySupervisor = TestActorRef[ClusterReadySupervisor](props)

      val expectedErrorMessage = "Error waiting for node to become ready, retrying"
      val msg = WaitForClusterToBeReady(dummyConnections)

      EventFilter.error(message = expectedErrorMessage,
        occurrences = ClusterReadyConfigTest.maxNrRetries + 1) intercept {
        clusterReadySupervisor ! msg
      }
    }

    "retry if cluster is not ready" in new TestConfig {

      implicit val executionContext: ExecutionContext = ClusterReadyConfigTest.executionContext

      val dummyConnections = Map.empty[NodeId, Connection.Salad]

      when(clusterOperations.isClusterReady(dummyConnections)).thenReturn(
        Future.successful(false)
      )

      val props = ClusterReadySupervisor.props
      val clusterReadySupervisor = TestActorRef[ClusterReadySupervisor](props)

      val expectedWarningMessagePattern = s"Cluster not yet ready, checking again in ${ClusterReadyConfigTest.backOffTime} seconds"
      val msg = WaitForClusterToBeReady(dummyConnections)

      EventFilter.warning(message = expectedWarningMessagePattern,
        occurrences = ClusterReadyConfigTest.maxNrRetries + 1) intercept {
        clusterReadySupervisor ! msg
      }
    }

    "become ready" in new TestConfig {
      import ClusterOperations._
      import com.adendamedia.cornucopia.redis.Connection

      implicit val executionContext: ExecutionContext = ClusterReadyConfigTest.executionContext

      val dummyConnections = Map.empty[NodeId, Connection.Salad]

      when(clusterOperations.isClusterReady(dummyConnections)).thenReturn(
        Future.successful(true)
      )

      val props = ClusterReadySupervisor.props
      val clusterReadySupervisor = TestActorRef[ClusterReadySupervisor](props)

      clusterReadySupervisor ! WaitForClusterToBeReady(dummyConnections)

      expectMsg {
        ClusterIsReady
      }
    }
  }

}

object ClusterReadyTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("ClusterReadyTest", config)
  }
}
