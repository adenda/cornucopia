package com.adendamedia.cornucopia

import akka.testkit.{ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors.{JoinRedisNode, JoinRedisNodeDelegate, MessageBus, Overseer}
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.CornucopiaException._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

class JoinRedisNodeTest extends TestKit(ActorSystem("JoinRedisNodeTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait Test {
    val uriString1: String = "redis://192.168.0.1"
    val uriString2: String = "redis://192.168.0.1"
    val redisURI1: RedisURI = RedisURI.create(uriString1)
    val redisURI2: RedisURI = RedisURI.create(uriString2)
    implicit val overseerMaxNrRetries: Int = 1
  }

  "JoinRedisNodeDelegate" must {
    "receive a Fail response from JoinRedisNodeDelegate when the connection to the Redis cluster fails" in new Test {
      import Overseer._
      import JoinRedisNode._

      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

      implicit val ec: ExecutionContext = system.dispatcher

      when(clusterOperations.addNodeToCluster(redisURI1)).thenReturn(Future.failed(CornucopiaRedisConnectionException("wat")))
      when(clusterOperations.addNodeToCluster(redisURI2)).thenReturn(Future.failed(CornucopiaRedisConnectionException("wat")))

      val joinRedisNodeDelegate = system.actorOf(JoinRedisNodeDelegate.props, "joinRedisNodeDelegate1")

      val msg1: JoinMasterNode = JoinMasterNode(redisURI1)
      val msg2: JoinSlaveNode = JoinSlaveNode(redisURI2)
      joinRedisNodeDelegate ! msg1
      joinRedisNodeDelegate ! msg2

      expectMsgAllOf(
        Fail(JoinMasterNode(redisURI1)),
        Fail(JoinSlaveNode(redisURI2))
      )
    }

    "receive a Passthrough response form JoinRedisNodeDelegate when the node is successfully added to the cluster" in new Test {
      import Overseer._
      import JoinRedisNode._

      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

      implicit val ec: ExecutionContext = system.dispatcher

      when(clusterOperations.addNodeToCluster(redisURI1)).thenReturn(Future.successful(redisURI1))
      when(clusterOperations.addNodeToCluster(redisURI2)).thenReturn(Future.successful(redisURI2))

      val joinRedisNodeDelegate = system.actorOf(JoinRedisNodeDelegate.props, "joinRedisNodeDelegate2")

      val msg1: JoinMasterNode = JoinMasterNode(redisURI1)
      val msg2: JoinSlaveNode = JoinSlaveNode(redisURI2)
      joinRedisNodeDelegate ! msg1
      joinRedisNodeDelegate ! msg2

      expectMsgAllOf(
        Passthrough(redisURI1),
        Passthrough(redisURI2)
      )
    }
  }

}
