package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, TestActorRef, TestKit, TestProbe, ImplicitSender, TestActors}
import akka.actor.{ActorSystem, ActorRefFactory}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors.{ReshardClusterSupervisor, GetRedisSourceNodes, ComputeReshardTable}
import com.adendamedia.cornucopia.redis.ClusterOperations
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.actors.MessageBus
import com.adendamedia.cornucopia.actors.Overseer
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar


class ReshardClusterTest extends TestKit(ActorSystem("ReshardClusterTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import Overseer._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait ReshardTest {
    val uriString1: String = "redis://192.168.0.100"
    val uriString2: String = "redis://192.168.0.200"
    val redisURI1: RedisURI = RedisURI.create(uriString1)
    val redisURI2: RedisURI = RedisURI.create(uriString2)
  }

  "GetRedisSourceNodes" must {

    "pipe target redis URI and source nodes to ComputeReshardTable actor" in new ReshardTest {

      val probe = TestProbe()
      val computeReshardTableFactory = (_: ActorRefFactory) => probe.ref

      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
      implicit val ec: ExecutionContext = system.dispatcher

      val dummyResult = List(new RedisClusterNode)

      when(clusterOperations.getRedisSourceNodes(redisURI1)).thenReturn(
        Future.successful(dummyResult)
      )

      val getRedisSourceNodesProps = GetRedisSourceNodes.props(computeReshardTableFactory)
      val getRedisSourceNodes = TestActorRef[GetRedisSourceNodes](getRedisSourceNodesProps)

      getRedisSourceNodes ! ReshardWithNewMaster(redisURI1)

      probe.expectMsg {
        (redisURI1, dummyResult)
      }
    }
  }
}
