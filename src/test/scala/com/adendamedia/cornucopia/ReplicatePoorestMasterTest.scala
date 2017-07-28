package com.adendamedia.cornucopia

import akka.testkit.{ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.ConfigNew.ReplicatePoorestMasterConfig
import com.adendamedia.cornucopia.actors.Overseer.{ReplicatePoorestMasterUsingSlave, ReplicatedMaster}
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ClusterOperations._
import com.adendamedia.cornucopia.redis.Connection

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

class ReplicatePoorestMasterTest extends TestKit(ActorSystem("ReplicatePoorestMasterTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    implicit object ReplicatePoorestMasterConfigTest extends ReplicatePoorestMasterConfig {
      val executionContext: ExecutionContext = system.dispatcher
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]
    val poorestMaster: NodeId = "foobar42"
    val dummyRedisUriToNodeId = Map.empty[RedisUriString, NodeId]

    val uriString: String = "redis://192.168.0.100"
    val newSlaveRedisURI: RedisURI = RedisURI.create(uriString)

    val retiredMasterUriString: String = "redis://192.168.0.101"
    val retiredMasterRedisURI: RedisURI = RedisURI.create(retiredMasterUriString)
    val excludedMasters = List(retiredMasterRedisURI)
  }

  "ReplicatePoorestMasterSupervisor" must {
    "succesfully replicate poorest master with new slave" in new TestConfig {

      implicit val executionContext: ExecutionContext = ReplicatePoorestMasterConfigTest.executionContext
      when(clusterOperations.findPoorestMaster(dummyConnections)).thenReturn(
        Future.successful(poorestMaster)
      )

      when(clusterOperations.replicateMaster(newSlaveRedisURI, poorestMaster, dummyConnections, dummyRedisUriToNodeId)).thenReturn(
        Future.successful()
      )

      val props = ReplicatePoorestMasterSupervisor.props
      val replicatePoorestMasterSupervisor = TestActorRef[ReplicatePoorestMasterSupervisor](props)

      val message = ReplicatePoorestMasterUsingSlave(newSlaveRedisURI, dummyConnections, dummyRedisUriToNodeId)

      replicatePoorestMasterSupervisor ! message

      expectMsg(
        ReplicatedMaster(newSlaveRedisURI)
      )
    }

    "successfully replicate poorest remaining master with existing slave" in new TestConfig {
      implicit val executionContext: ExecutionContext = ReplicatePoorestMasterConfigTest.executionContext

      when(clusterOperations.findPoorestRemainingMaster(dummyConnections, excludedMasters)).thenReturn(
        Future.successful(poorestMaster)
      )


    }
  }


}

