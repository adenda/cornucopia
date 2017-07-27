package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ReshardTableNew
import com.adendamedia.cornucopia.ConfigNew.GetSlavesOfMasterConfig
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.actors.Overseer._
import org.scalatest.mockito.MockitoSugar

class GetSlavesOfMasterTest extends TestKit(ActorSystem("GetSlavesOfMasterTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  trait TestConfig {
    implicit object GetSlavesOfMasterTestConfig extends GetSlavesOfMasterConfig {
      val executionContext: ExecutionContext = system.dispatcher
      val maxNrRetries: Int = 2
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
    val dummySlaves: List[RedisClusterNode] = List(new RedisClusterNode)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  "GetSlavesOfMasterSupervisor" must {
    "010 - get slaves of master" in new TestConfig {
      implicit val executionContext: ExecutionContext = system.dispatcher
      when(clusterOperations.getSlavesOfMaster(redisURI)).thenReturn(Future.successful(dummySlaves))

      val getSlavesOfMasterSupervisor = TestActorRef[GetSlavesOfMasterSupervisor](GetSlavesOfMasterSupervisor.props)

      val message = GetSlavesOf(redisURI)

      getSlavesOfMasterSupervisor ! message

      expectMsg(GotSlavesOf(redisURI, dummySlaves))
    }
  }

}
