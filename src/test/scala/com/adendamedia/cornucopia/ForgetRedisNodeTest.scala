package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ReshardTableNew
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ClusterOperations._
import com.adendamedia.cornucopia.ConfigNew.ForgetRedisNodeConfig
import org.scalatest.mockito.MockitoSugar
import ReshardClusterTest._
import com.adendamedia.cornucopia.redis.ReshardTableNew.ReshardTableType
import ForgetRedisNodeTest.testSystem
import Overseer._

class ForgetRedisNodeTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    implicit object ForgetRedisNodeConfigTest extends ForgetRedisNodeConfig {
      val executionContext: ExecutionContext = system.dispatcher
      val maxNrRetries: Int = 2
    }
    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]
    val dummyRedisUriToNodeId = Map.empty[RedisUriString, NodeId]
    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
  }

  "ForgetRedisNodeSupervisor" must {
    "010 - forget redis node" in new TestConfig {

      implicit val executionContext: ExecutionContext = ForgetRedisNodeConfigTest.executionContext
      when(clusterOperations.forgetNode(redisURI, dummyConnections, dummyRedisUriToNodeId)).thenReturn(
        Future.successful()
      )

      val props = ForgetRedisNodeSupervisor.props
      val forgetRedisNodeSupervisor = TestActorRef[ForgetRedisNodeSupervisor](props)

      val msg = ForgetNode(redisURI, dummyConnections, dummyRedisUriToNodeId)

      forgetRedisNodeSupervisor ! msg

      expectMsg(
        NodeForgotten(redisURI)
      )
    }

    "020 - retry if it fails to forget redis node" in new TestConfig {
      implicit val executionContext: ExecutionContext = ForgetRedisNodeConfigTest.executionContext
      when(clusterOperations.forgetNode(redisURI, dummyConnections, dummyRedisUriToNodeId)).thenReturn(
        Future.failed(CornucopiaForgetNodeException("wat"))
      )

      val props = ForgetRedisNodeSupervisor.props
      val forgetRedisNodeSupervisor = TestActorRef[ForgetRedisNodeSupervisor](props)

      val msg = ForgetNode(redisURI, dummyConnections, dummyRedisUriToNodeId)

      val expectedMessage = s"Retrying to forget redis node"

      EventFilter.info(message = expectedMessage,
        occurrences = ForgetRedisNodeConfigTest.maxNrRetries + 1) intercept {
        forgetRedisNodeSupervisor ! msg
      }
    }
  }

}

object ForgetRedisNodeTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("ForgetRedisNodeTest", config)
  }
}
