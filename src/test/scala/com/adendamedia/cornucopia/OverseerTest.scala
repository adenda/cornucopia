package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, TestActorRef, TestKit, TestProbe, ImplicitSender, TestActors}
import akka.actor.{ActorSystem, ActorRefFactory, ActorRef}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors.JoinRedisNodeSupervisor
import com.adendamedia.cornucopia.redis.ClusterOperations
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors.MessageBus
import com.adendamedia.cornucopia.actors.Overseer
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar

import OverseerTest._

class OverseerTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import MessageBus._
  import Overseer._

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait Test {
    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
    implicit val joinRedisNodeMaxNrRetries: Int = 2
    val cornucopiaRedisConnectionExceptionMessage = "wat"

    implicit val ec: ExecutionContext = system.dispatcher
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
    val addMasterNodeMessage: AddNode = AddMaster(redisURI)
    val addSlaveNodeMessage: AddNode = AddSlave(redisURI)
  }

  trait FailureTest extends Test {
    import ClusterOperations._

    when(clusterOperations.addNodeToCluster(redisURI))
      .thenReturn(
        Future.failed(CornucopiaRedisConnectionException(cornucopiaRedisConnectionExceptionMessage))
      )

    val joinRedisNodeSupervisorMaker =
      (f: ActorRefFactory) => f.actorOf(JoinRedisNodeSupervisor.props, "joinRedisNodeSupervisor1")

    val reshardClusterSupervisorMaker =
      (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

    val dummy1 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
    val dummy2 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
    val dummy3 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

    val overseerProps = Overseer.props(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, dummy1, dummy2, dummy3)
    val overseer = TestActorRef[Overseer](overseerProps)
  }

  trait SuccessTest extends Test {
    when(clusterOperations.addNodeToCluster(redisURI)).thenReturn(Future.successful(redisURI))
    val joinRedisNodeSupervisor = TestActorRef[JoinRedisNodeSupervisor](JoinRedisNodeSupervisor.props)
  }

  trait MigrateTest {
    import com.adendamedia.cornucopia.redis.ClusterOperations._
    import com.adendamedia.cornucopia.redis.Connection
    import com.adendamedia.cornucopia.redis.ReshardTableNew._

    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
    val testTargetNodeId = "target1"
    val testRedisUriToNodeId: Map[RedisUriString, NodeId] = Map(uriString -> testTargetNodeId)
    val dummyConnections: ClusterConnectionsType = Map.empty[NodeId, Connection.Salad]

    val reshardTableMock: ReshardTableType = Map("node1" -> List(1, 2), "node2" -> List(3,4,5), "node3" -> List(6,7))

    val reshardTableMockEmpty: ReshardTableType = Map.empty[NodeId, List[Slot]]
  }

  "Overseer" must {
    "retry joining node to cluster if it fails" in new FailureTest {
      val expectedErrorMessage =
        s"Failed to join node ${redisURI.toURI} with error: $cornucopiaRedisConnectionExceptionMessage"

      EventFilter.error(message = expectedErrorMessage,
        occurrences = joinRedisNodeMaxNrRetries + 1) intercept {
          system.eventStream.publish(addMasterNodeMessage)
        }
    }

    "fail to join node to cluster after maximum number of retries is reached" in new FailureTest {
      val expectedErrorMessage =
        s"Could not join Redis node to cluster after $joinRedisNodeMaxNrRetries retries: Restarting child actor"

      EventFilter.error(message = expectedErrorMessage,
        occurrences = 1) intercept {
        system.eventStream.publish(addMasterNodeMessage)
      }
    }

    "publish to event bus when it fails to add a node to the cluster" in new FailureTest {
      val probe = TestProbe()

      system.eventStream.subscribe(probe.ref, classOf[FailedAddingMasterRedisNode])

      system.eventStream.publish(addMasterNodeMessage)

      val msg = FailedAddingMasterRedisNode(
        s"Could not join Redis node to cluster after $joinRedisNodeMaxNrRetries retries"
      )

      probe.expectMsg(msg)
    }

    "succeed joining master node to cluster" in new SuccessTest {
      joinRedisNodeSupervisor ! JoinMasterNode(redisURI)

      expectMsg(MasterNodeJoined(redisURI))
    }

    "succeed joining slave node to cluster" in new SuccessTest {
      joinRedisNodeSupervisor ! JoinSlaveNode(redisURI)

      expectMsg(SlaveNodeJoined(redisURI))
    }

    "receive add master node task from message bus and tell JoinRedisNodeSupervisor actor with JoinMasterNode command" in new Test {
      import Overseer._
      import MessageBus._

      val probe = TestProbe()

      val joinRedisNodeSupervisorMaker = (f: ActorRefFactory) => probe.ref
      val reshardClusterSupervisorMaker = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

      val dummy1 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy2 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy3 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

      val overseerProps = Overseer.props(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, dummy1, dummy2, dummy3)
      val overseer = TestActorRef[Overseer](overseerProps)

      val msg: AddNode = AddMaster(redisURI)
      system.eventStream.publish(msg)

      probe.expectMsgPF() {
        case JoinMasterNode(uri: RedisURI) =>
          uri must be(redisURI)
      }
    }

    "receive add slave node task from the message bus and tell JoinRedisNodeSupervisor actor with JoinMasterNode command" in new Test {
      import Overseer._
      import MessageBus._

      val probe = TestProbe()

      val joinRedisNodeSupervisorMaker = (f: ActorRefFactory) => probe.ref

      val dummy1 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy2 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy3 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy4 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

      val overseerProps = Overseer.props(joinRedisNodeSupervisorMaker, dummy1, dummy2, dummy3, dummy4)
      val overseer = TestActorRef[Overseer](overseerProps)

      system.eventStream.publish(addSlaveNodeMessage)

      probe.expectMsgPF() {
        case JoinSlaveNode(uri: RedisURI) =>
          uri must be(redisURI)
      }
    }

    "tell reshard cluster supervisor to reshard with new master after master node is joined to cluster" in new Test {
      import Overseer._

      val probe = TestProbe()

      val joinRedisNodeSupervisorMaker = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val reshardClusterSupervisorMaker = (f: ActorRefFactory) => probe.ref

      val dummy1 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy2 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)
      val dummy3 = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

      val overseerProps = Overseer.props(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker, dummy1, dummy2, dummy3)
      val overseer = TestActorRef[Overseer](overseerProps)

      overseer ! AddMaster(redisURI)
      overseer ! MasterNodeJoined(redisURI)

      probe.expectMsg(ReshardWithNewMaster(redisURI))
    }

    "020 - publish to event bus when the migrate slots job has completed successfully" in new MigrateTest {
      val probe = TestProbe()
      system.eventStream.subscribe(probe.ref, classOf[MasterNodeAdded])

      val migrateMessage = MigrateSlotsForNewMaster(redisURI, dummyConnections, testRedisUriToNodeId, reshardTableMockEmpty)

      val blackHole = (f: ActorRefFactory) => f.actorOf(TestActors.blackholeProps)

      val props = Overseer.props(blackHole, blackHole, blackHole, blackHole, blackHole)
      val overseer = TestActorRef[Overseer](props)

      overseer ! AddMaster(redisURI)
      overseer ! MasterNodeJoined(redisURI)
      overseer ! GotClusterConnections(dummyConnections, testRedisUriToNodeId)
      overseer ! GotReshardTable(reshardTableMockEmpty)
      overseer ! ClusterIsReady
      overseer ! JobCompleted(migrateMessage)

      val msg = MasterNodeAdded(redisURI)
      probe.expectMsg(msg)
    }

  }

}

object OverseerTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("OverseerTest", config)
  }
}
