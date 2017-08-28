package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem, ActorRef}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.Config._
import com.adendamedia.cornucopia.actors.MigrateSlotsJobManager._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ReshardTable._
import com.adendamedia.cornucopia.Config.ClusterReadyConfig
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
      val maxNrRetries: Int = 2
      val numberOfWorkers: Int = 2
      val setSlotAssignmentRetryBackoff: Int = 0
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]

    val testTargetNodeId = "target1"

    val testRedisUriToNodeId: Map[RedisURI, NodeId] = Map(redisURI -> testTargetNodeId)

    val testNodeIdToRedisUri: Map[NodeId, RedisURI] = Map(testTargetNodeId -> redisURI, "node1" -> redisURI, "node2" -> redisURI)

    val dummySaladApi: Connection.SaladAPI = mock[Connection.SaladAPI]

    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]
  }

  trait JobManagerTestConfig extends TestConfig {
    implicit object Config extends MigrateSlotsConfig {
      val executionContext: ExecutionContext = system.dispatcher
      val maxNrRetries: Int = 1
      val numberOfWorkers: Int = 1
      val setSlotAssignmentRetryBackoff: Int = 0
    }
    val migrateSlotWorkerMaker = (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)
    val props = MigrateSlotsSupervisor.props(migrateSlotWorkerMaker)
    val migrateSlotsSupervisor = TestActorRef[MigrateSlotsSupervisor[MigrateSlotsCommand]](props)
    implicit val ec: ExecutionContext = Config.executionContext

    when(
      clusterOperations.setSlotAssignment(1, "node1", testTargetNodeId, dummyConnections)
    ).thenReturn(
      Future.failed(SetSlotAssignmentException("wat"))
    )

    when(
      clusterOperations.setSlotAssignment(2, "node2", testTargetNodeId, dummyConnections)
    ).thenReturn(
      Future.successful()
    )

    when(
      clusterOperations.setSlotAssignment(1, testTargetNodeId, "node1", dummyConnections)
    ).thenReturn(
      Future.failed(SetSlotAssignmentException("wat"))
    )

    when(
      clusterOperations.setSlotAssignment(2, testTargetNodeId, "node2", dummyConnections)
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

    val reshardTableMock: ReshardTableType = Map("node1" -> List(1), "node2" -> List(2))
  }

  trait SuccessTestForAddingNewMaster extends TestConfig {
    implicit val executionContext: ExecutionContext = MigrateSlotsConfigTest.executionContext

    val reshardTableMock: ReshardTableType = Map("node1" -> List(1, 2),
      "node2" -> List(3,4,5),
      "node3" -> List(6,7))

    val migrateSlotWorkerMaker = (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)

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
    override val testNodeIdToRedisUri: Map[NodeId, RedisURI] = Map("node1" -> redisURI, "node2" -> redisURI, "node3" -> redisURI)
    val testSourceNodeId = "sourceNodeId"
    override val testRedisUriToNodeId: Map[RedisURI, NodeId] = Map(redisURI -> testSourceNodeId)

    val migrateSlotWorkerMaker = (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)

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

      val dummyReshardTable = Map.empty[NodeId, List[Slot]]

      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummySaladApi, dummyReshardTable)

      migrateSlotJobManager ! msg

      probe.expectMsgAllOf(GetJob, GetJob)
    }

    "081 - retry to set slot assignment if slot assignment fails" in new TestConfig {
      private val dummyManager = TestActorRef(TestActors.blackholeProps)
      private val worker = TestActorRef[MigrateSlotWorker](MigrateSlotWorker.props(dummyManager))

      when(
        clusterOperations.setSlotAssignment(anyInt(), anyString(), anyString(), anyObject())(anyObject())
      ).thenReturn(
        Future.failed(SetSlotAssignmentException("wat"))
      )

      private val slot: Slot = 42

      val msg = MigrateSlotJob("source", "target", slot, dummyConnections, Some(redisURI))

      val logMsg = s"Retrying to set slot assignment for slot $slot"

      EventFilter.info(message = logMsg, occurrences = MigrateSlotsConfigTest.maxNrRetries + 1) intercept {
        worker ! msg
      }
    }
  }

  "MigrateSlotsJobManager" should {
    "010 - migrate slots for adding new master" in new SuccessTestForAddingNewMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummySaladApi, reshardTableMock)

      val pat = s"Successfully migrated slot \\d from node\\d to $testTargetNodeId"

      EventFilter.debug(pattern = pat,
        occurrences = 7) intercept {
        migrateSlotsJobManager ! msg
      }

    }

    "020 - migrate slots for removing retired master" in new SuccessTestForRemovingRetiredMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsWithoutRetiredMaster(redisURI, dummyConnections, testRedisUriToNodeId, testNodeIdToRedisUri, dummySaladApi, reshardTableMock)

      val pat = s"Successfully migrated slot \\d from $testSourceNodeId to node\\d"

      EventFilter.debug(pattern = pat,
        occurrences = 7) intercept {
        migrateSlotsJobManager ! msg
      }
    }

    "030 - signal to supervisor once it has completed its jobs" in new SuccessTestForAddingNewMaster {
      val props = MigrateSlotsJobManager.props(migrateSlotWorkerMaker)
      val migrateSlotsJobManager = TestActorRef[MigrateSlotsJobManager](props)
      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummySaladApi, reshardTableMock)

      migrateSlotsJobManager ! msg

      expectMsgPF() {
        case JobCompleted(_) => true
      }
    }

    "090 - Fail job when the job fails during the set slot assignment stage when adding a new master but continue to process other jobs" in new JobManagerTestConfig {
      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummySaladApi, reshardTableMock)

      val message = s"The following slot migration jobs failed: (node1,1)"
      val expectedOccurrences = 1

      EventFilter.warning(message = message, occurrences = 1) intercept {
        migrateSlotsSupervisor ! msg
      }
    }

    "091 - Fail job when the job fails during the set slot assignment stage when retiring an old master but continue to process other jobs" in new JobManagerTestConfig {
      val msg = MigrateSlotsWithoutRetiredMaster(redisURI, dummyConnections, testRedisUriToNodeId, testNodeIdToRedisUri, dummySaladApi, reshardTableMock)

      val message = s"The following slot migration jobs failed: (node1,1)"
      val expectedOccurrences = 1

      EventFilter.warning(message = message, occurrences = 1) intercept {
        migrateSlotsSupervisor ! msg
      }
    }

    "092 - Fail job when the job fails during the slot keys migrations stage but continue to process other jobs" in new JobManagerTestConfig {

      when(
        clusterOperations.setSlotAssignment(anyInt(), anyString(), anyString(), anyObject())(anyObject())
      ).thenReturn(
        Future.successful()
      )

      when(
        clusterOperations.migrateSlotKeys(anyInt(), anyObject(), anyString(), anyString(), anyObject())(anyObject())
      ).thenReturn(
        // Any MigrateSlotsException is handled in the same way
        Future.failed(MigrateSlotKeysClusterDownException(reason = new Exception("wat")))
      )

//      val reshardTableMock: ReshardTableType = Map("node1" -> List(1), "node2" -> List(2))

      val msg = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, dummySaladApi, reshardTableMock)

      // TODO: Correct the log message to indicate
      val message = s"Migrate slot job failed while trying to set slot assignment for job .*"

      EventFilter[FailedSlotMigrationJobException](pattern = message, occurrences = reshardTableMock.size) intercept {
        migrateSlotsSupervisor ! msg
      }
    }
  }
}

object MigrateSlotsTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loglevel = "DEBUG"
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("MigrateSlotsTest", config)
  }
}

