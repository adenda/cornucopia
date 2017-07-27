package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors.{ComputeReshardTable, GetRedisSourceNodes, ReshardClusterSupervisor}
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.ReshardTableNew
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
import ReshardClusterTest._
import com.adendamedia.cornucopia.redis.ReshardTableNew.ReshardTableType

class ReshardClusterTest extends TestKit(testSystem)
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

  trait ReshardClusterConfigTest {
    import com.adendamedia.cornucopia.ConfigNew.ReshardClusterConfig
    implicit object ReshardClusterConfigTest extends ReshardClusterConfig {
      val maxNrRetries: Int = 2
      override val expectedTotalNumberSlots: Int = 42 // doesn't matter we're not testing this here
      val executionContext: ExecutionContext = system.dispatcher
    }
  }

  "ReshardClusterSupervisor" must {
    "010 - receive the reshard table and forward it to its sender when resharding with new master" in new ReshardTest with ReshardClusterConfigTest {
      import ReshardTableNew._
      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
      val dummySourceNodes = List(new RedisClusterNode)

      implicit val ec: ExecutionContext = system.dispatcher

      when(clusterOperations.getRedisSourceNodes(redisURI1)).thenReturn(
        Future.successful(dummySourceNodes)
      )

      val dummyReshardTable: ReshardTableType = Map.empty[NodeId, List[Slot]]

      implicit val reshardTable: ReshardTableNew = mock[ReshardTableNew]
      implicit val expectedTotalNumberSlots: Int = ReshardClusterConfigTest.expectedTotalNumberSlots
      when(reshardTable.computeReshardTable(dummySourceNodes)).thenReturn(dummyReshardTable)

      val computeReshardTableFactory =
        (f: ActorRefFactory) => f.actorOf(ComputeReshardTable.props, ComputeReshardTable.name)

      val props = ReshardClusterSupervisor.props(computeReshardTableFactory)
      val reshardClusterSupervisor = TestActorRef[ReshardClusterSupervisor](props)

      reshardClusterSupervisor ! ReshardWithNewMaster(redisURI1)

      expectMsg(
        GotReshardTable(dummyReshardTable)
      )
    }
  }

  "ReshardClusterSupervisor" must {
    "020 - receive the reshard table and forward it to its sender when resharding without retired master" in new ReshardTest with ReshardClusterConfigTest {
      import ReshardTableNew._
      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
      val dummyTargetNodes = List(new RedisClusterNode)
      val dummySourceNode = new RedisClusterNode

      implicit val ec: ExecutionContext = system.dispatcher

      when(clusterOperations.getRedisTargetNodesAndRetiredNode(redisURI1)).thenReturn(
        Future.successful((dummyTargetNodes, dummySourceNode))
      )

      val dummyReshardTable: ReshardTableType = Map.empty[NodeId, List[Slot]]

      implicit val reshardTable: ReshardTableNew = mock[ReshardTableNew]
      implicit val expectedTotalNumberSlots: Int = ReshardClusterConfigTest.expectedTotalNumberSlots
      when(reshardTable.computeReshardTablePrime(dummySourceNode, dummyTargetNodes)).thenReturn(dummyReshardTable)

      val computeReshardTableFactory =
        (f: ActorRefFactory) => f.actorOf(ComputeReshardTable.props, ComputeReshardTable.name)

      val props = ReshardClusterSupervisor.props(computeReshardTableFactory)
      val reshardClusterSupervisor = TestActorRef[ReshardClusterSupervisor](props, ReshardClusterSupervisor.name)

      reshardClusterSupervisor ! ReshardWithoutRetiredMaster(redisURI1)

      expectMsg(
        GotReshardTable(dummyReshardTable)
      )
    }
  }

  "GetRedisSourceNodes" must {

    "pipe target redis URI and source nodes to ComputeReshardTable actor" in new ReshardTest with ReshardClusterConfigTest {
      import GetRedisSourceNodes._

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
        (redisURI1, SourceNodes(dummyResult))
      }
    }
  }

  "ReshardClusterSupervisor" should {
    "Retry computing reshard table for new master if there is an error" in new ReshardTest with ReshardClusterConfigTest {
      // NOTE: ReshardClusterSupervisor is the grand-parent of ComputeReshardTable actor, and the error is escalated
      //       from its child
      import ReshardTableNew._

      val sourceNodes = List(new RedisClusterNode)
      val reshardTableExceptionMessage = "wat"

      implicit val reshardTable: ReshardTableNew = mock[ReshardTableNew]
      implicit val expectedTotalNumberSlots: Int = ReshardClusterConfigTest.expectedTotalNumberSlots
      when(reshardTable.computeReshardTable(sourceNodes))
        .thenThrow(ReshardTableException(reshardTableExceptionMessage))

      val computeReshardTableFactory = (f: ActorRefFactory) => f.actorOf(ComputeReshardTable.props, ComputeReshardTable.name)

      implicit val ec: ExecutionContext = ReshardClusterConfigTest.executionContext
      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
      when(clusterOperations.getRedisSourceNodes(redisURI1)).thenReturn(
        Future.successful(sourceNodes)
      )

      val reshardClusterSupervisorProps = ReshardClusterSupervisor.props(computeReshardTableFactory)
      val reshardClusterSupervisor = TestActorRef[ReshardClusterSupervisor](reshardClusterSupervisorProps)

      val msg = ReshardWithNewMaster(redisURI1)

      val expectedErrorMessage =
        s"Computing reshard table to add new master ${redisURI1.toURI}"

      EventFilter.info(message = expectedErrorMessage,
        occurrences = ReshardClusterConfigTest.maxNrRetries + 1) intercept {
        reshardClusterSupervisor ! msg
      }
    }
  }
}

object ReshardClusterTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("ReshardClusterTest", config)
  }
}
