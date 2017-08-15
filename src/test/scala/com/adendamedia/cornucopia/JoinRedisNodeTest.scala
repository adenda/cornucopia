package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.Config.JoinRedisNodeConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._

import JoinRedisNodeTest._

class JoinRedisNodeTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait Test {
    val uriString1: String = "redis://192.168.0.100"
    val uriString2: String = "redis://192.168.0.200"
    val redisURI1: RedisURI = RedisURI.create(uriString1)
    val redisURI2: RedisURI = RedisURI.create(uriString2)
    implicit val overseerMaxNrRetries: Int = 1
  }

  trait TestConfig {
    implicit object config extends JoinRedisNodeConfig {
      val maxNrRetries: Int = 2
      val refreshTimeout: Int = 0
      val retryBackoffTime: Int = 0
      val executionContext: ExecutionContext = system.dispatcher
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
  }

  trait FailureTest extends Test with TestConfig {
    import ClusterOperations._

    implicit val ec: ExecutionContext = config.executionContext

    when(clusterOperations.addNodeToCluster(redisURI1)).thenReturn(Future.failed(CornucopiaRedisConnectionException("wat")))
    when(clusterOperations.addNodeToCluster(redisURI2)).thenReturn(Future.failed(CornucopiaRedisConnectionException("wat")))
  }

  trait SuccessTest extends Test with TestConfig {
    implicit val ec: ExecutionContext = config.executionContext

    when(clusterOperations.addNodeToCluster(redisURI1)).thenReturn(
      Future.successful(redisURI1)
    )
    when(clusterOperations.addNodeToCluster(redisURI2)).thenReturn(
      Future.successful(redisURI2)
    )

    protected val joinRedisNodeSupervisor: TestActorRef[JoinRedisNodeSupervisor[_]] =
      TestActorRef[JoinRedisNodeSupervisor[_]](JoinRedisNodeSupervisor.props)
  }

  "JoinRedisNodeDelegate" must {
    "receive a Fail response from JoinRedisNodeDelegate when the connection to the Redis cluster fails" in new FailureTest {

      import Overseer._
      import JoinRedisNode._

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

  }

  "JoinRedisNodeSupervisor" must {
    "050 - retry joining redis node if there is a cluster failure" in new FailureTest {
      import Overseer._

      private val joinRedisNodeSupervisor = TestActorRef[JoinRedisNodeSupervisor[_]](JoinRedisNodeSupervisor.props)
      private val msg = JoinMasterNode(redisURI1)

      val logMsg = s"Retrying to join redis node $redisURI1"

      EventFilter.info(message = logMsg, occurrences = config.maxNrRetries + 1) intercept {
        joinRedisNodeSupervisor ! msg
      }
    }

    "052 - join master redis node" in new SuccessTest {
      import Overseer._

      private val joinMasterMsg = JoinMasterNode(redisURI1)

      joinRedisNodeSupervisor ! joinMasterMsg

      expectMsg(
        MasterNodeJoined(redisURI1)
      )
    }

    "053 - join slave redis node" in new SuccessTest {
      import Overseer._

      private val joinSlaveMsg = JoinSlaveNode(redisURI2)

      joinRedisNodeSupervisor ! joinSlaveMsg

      expectMsg(
        SlaveNodeJoined(redisURI2)
      )
    }
  }

  "JoinRedisNode" must {
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

object JoinRedisNodeTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loglevel = "INFO"
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("JoinRedisNodeTest", config)
  }
}
