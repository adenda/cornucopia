package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem, ActorRef}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.ConfigNew._
import com.adendamedia.cornucopia.actors.MigrateSlotsJobManager._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.adendamedia.cornucopia.ConfigNew.ClusterReadyConfig
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar

import MigrateSlotsTest._

class MigrateSlotsTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  import Overseer._
  import ClusterOperations._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    val uriString1: String = "redis://192.168.0.100"
    val uriString2: String = "redis://192.168.0.200"
    val redisURI1: RedisURI = RedisURI.create(uriString1)
    val redisURI2: RedisURI = RedisURI.create(uriString2)
    implicit object MigrateSlotsConfigTest extends MigrateSlotsConfig {
      val executionContext: ExecutionContext = system.dispatcher
      val maxNrRetries: Int = 10
      val numberOfWorkers: Int = 2
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

    val testRedisUriToNodeId: Map[RedisUriString, NodeId] = Map(uriString1 -> "1", uriString2 -> "2")
  }

  "MigrateSlotWorker" must {
    "ask for a job" in new TestConfig {
      val probe = TestProbe()

      val migrateSlotWorkerMaker = (f: ActorRefFactory, _: ActorRef) => f.actorOf(MigrateSlotWorker.props(probe.ref), MigrateSlotWorker.name)

      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotJobManager = TestActorRef[MigrateSlotsJobManager](props)

      val dummyConnections = Map.empty[NodeId, Connection.Salad]
      val dummyReshardTable = Map.empty[NodeId, List[Slot]]

      val msg = MigrateSlotsForNewMaster(redisURI1, dummyConnections, testRedisUriToNodeId, dummyReshardTable)

      migrateSlotJobManager ! msg

      probe.expectMsgAllOf(GetJob, GetJob)
    }
  }

}

object MigrateSlotsTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("MigrateSlotsTest", config)
  }
}
