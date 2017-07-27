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
import org.mockito.Matchers._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar

import MigrateSlotsTest._

class MigrateSlotsTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import Overseer._
  import ClusterOperations._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait TestConfig {
    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
    implicit object MigrateSlotsConfigTest extends MigrateSlotsConfig {
      val executionContext: ExecutionContext = system.dispatcher
      val maxNrRetries: Int = 10
      val numberOfWorkers: Int = 2
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

    val testTargetNodeId = "target1"

    val testRedisUriToNodeId: Map[RedisUriString, NodeId] = Map(redisURI.toString -> testTargetNodeId)
  }

  trait SuccessTestForAddingNewMaster extends TestConfig {
    implicit val executionContext: ExecutionContext = MigrateSlotsConfigTest.executionContext

    val reshardTableMock: ReshardTableType = Map("node1" -> List(1, 2),
      "node2" -> List(3,4,5),
      "node3" -> List(6,7))

    val migrateSlotWorkerMaker = (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)

    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]

    when(
      clusterOperations.setSlotAssignment(anyInt(), anyString(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )

    when(
      clusterOperations.migrateSlotKeys(anyInt(), anyObject(), anyString(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )

    when(
      clusterOperations.notifySlotAssignment(anyInt(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )
  }

  trait SuccessTestForRemovingRetiredMaster extends TestConfig {
    implicit val executionContext: ExecutionContext = MigrateSlotsConfigTest.executionContext
    // This means that the three nodes are *receiving* the slots from the associated lists
    val reshardTableMock: ReshardTableType = Map(
      "node1" -> List(1, 2),
      "node2" -> List(3,4,5),
      "node3" -> List(6,7)
    )

    val migrateSlotWorkerMaker = (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)

    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]

    when(
      clusterOperations.setSlotAssignment(anyInt(), anyString(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )

    when(
      clusterOperations.migrateSlotKeys(anyInt(), anyObject(), anyString(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )

    when(
      clusterOperations.notifySlotAssignment(anyInt(), anyString(), anyObject())(anyObject())
    ).thenReturn(
      Future.successful()
    )
  }

  "MigrateSlotWorker" must {
    "ask for a job" in new TestConfig {
      val probe = TestProbe()

      val migrateSlotWorkerMaker = (f: ActorRefFactory, _: ActorRef) => f.actorOf(MigrateSlotWorker.props(probe.ref), MigrateSlotWorker.name)

      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotJobManager = TestActorRef[MigrateSlotsJobManager](props)

      val dummyConnections = Map.empty[NodeId, Connection.Salad]
      val dummyReshardTable = Map.empty[NodeId, List[Slot]]

      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummyReshardTable)

      migrateSlotJobManager ! msg

      probe.expectMsgAllOf(GetJob, GetJob)
    }
  }

  "MigrateSlotsJobManager" should {
    "migrate slots for adding new master" in new SuccessTestForAddingNewMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, reshardTableMock)

      val pat = s"Successfully migrated slot \\d from node\\d to $testTargetNodeId"

      EventFilter.info(pattern = pat,
        occurrences = 7) intercept {
        migrateSlotsJobManager ! msg
      }

    }

    "migrate slots for removing retired master" in new SuccessTestForRemovingRetiredMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsWithoutRetiredMaster(redisURI, dummyConnections, testRedisUriToNodeId, reshardTableMock)

      val pat = s"Successfully migrated slot \\d from node\\d to $testTargetNodeId"

      EventFilter.info(pattern = pat,
        occurrences = 7) intercept {
        migrateSlotsJobManager ! msg
      }
    }
  }

  "MigrateSlotsJobManager" should {
    "030 - signal to supervisor once it has completed its jobs" in new SuccessTestForAddingNewMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, reshardTableMock)

      migrateSlotsJobManager ! msg

      expectMsg(JobCompleted(msg))
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
