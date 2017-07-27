package com.adendamedia.cornucopia

import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRefFactory, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.redis.Connection
import com.adendamedia.cornucopia.redis.ReshardTableNew
import com.adendamedia.cornucopia.ConfigNew.FailoverConfig
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar
import FailoverTest._
import com.adendamedia.cornucopia.actors.FailoverSupervisor.{DoFailover, FailoverComplete}

class FailoverTest extends TestKit(testSystem)
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar with ImplicitSender {

  import Overseer._
  import ClusterOperations._

  override def afterAll(): Unit = {
    system.terminate()
  }


  trait TestConfig {
    implicit object FailoverConfigTest extends FailoverConfig {
      val maxNrRetries: Int = 1
      val executionContext: ExecutionContext = system.dispatcher
      val verificationRetryBackOffTime: Int = 1
      val maxNrAttemptsToVerify: Int = 1
    }
    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
    val uriString: String = "redis://192.168.0.100"
    val redisURI: RedisURI = RedisURI.create(uriString)
  }

  "FailoverSupervisor" must {
    "030 - Failover so that the given node that is a slave becomes a master" in new TestConfig {
      implicit val executionContext: ExecutionContext = system.dispatcher
      when(clusterOperations.getRole(redisURI)).thenReturn(Future.successful(Slave))
      when(clusterOperations.failoverMaster(redisURI)).thenReturn(Future.successful())
      when(clusterOperations.verifyFailover(redisURI, Master)).thenReturn(Future.successful(true))

      val failoverSupervisor = TestActorRef[FailoverSupervisor](FailoverSupervisor.props)

      val msg = FailoverMaster(redisURI)

      failoverSupervisor ! msg

      expectMsg(FailoverComplete)
    }

    "035 - Failover so that the given node that is a master becomes a slave" in new TestConfig {
      implicit val executionContext: ExecutionContext = system.dispatcher
      when(clusterOperations.getRole(redisURI)).thenReturn(Future.successful(Master))
      when(clusterOperations.failoverSlave(redisURI)).thenReturn(Future.successful())
      when(clusterOperations.verifyFailover(redisURI, Slave)).thenReturn(Future.successful(true))

      val failoverSupervisor = TestActorRef[FailoverSupervisor](FailoverSupervisor.props)

      val msg = FailoverSlave(redisURI)

      failoverSupervisor ! msg

      expectMsg(FailoverComplete)
    }

    "037 - Retry failover when the failover was not verified within the allotted number of retries" in new TestConfig {
      implicit val executionContext: ExecutionContext = system.dispatcher
      when(clusterOperations.getRole(redisURI)).thenReturn(Future.successful(Master))
      when(clusterOperations.failoverSlave(redisURI)).thenReturn(Future.successful())
      when(clusterOperations.verifyFailover(redisURI, Slave)).thenReturn(Future.successful(false))

      val failoverSupervisor = TestActorRef[FailoverSupervisor](FailoverSupervisor.props)

      val msg = FailoverSlave(redisURI)

      val retries = FailoverConfigTest.maxNrRetries
      val attempts = FailoverConfigTest.maxNrAttemptsToVerify
      val expectedMessage = s"Failed verifying failover by exceeding maximum retry attempts of $attempts"

      EventFilter.error(pattern = expectedMessage, occurrences = retries + 1) intercept {
        failoverSupervisor ! msg
      }
    }

    "Failover" must {

      "020 - short-circuit when failing over a master that is already a master" in new TestConfig {
        val blackHole = TestActorRef(TestActors.blackholeProps)
        val failover = TestActorRef[Failover](Failover.props(blackHole))
        val failoverMaster = FailoverMaster(redisURI)
        val msg = DoFailover(failoverMaster, Master)

        failover ! msg

        expectMsg(FailoverComplete)
      }

      "022 - short-circuit when failing over a slave that is already a slave" in new TestConfig {
        val blackHole = TestActorRef(TestActors.blackholeProps)
        val failover = TestActorRef[Failover](Failover.props(blackHole))
        val failoverSlave = FailoverSlave(redisURI)
        val msg = DoFailover(failoverSlave, Slave)

        failover ! msg

        expectMsg(FailoverComplete)
      }

      "025 - Retry failover verification when the verification fails normally" in new TestConfig {
        // WARNING: This test needs to be last in this suite because there is an uncaught exception that will bring down
        // the test actor system.
        implicit val executionContext: ExecutionContext = system.dispatcher
        when(clusterOperations.verifyFailover(redisURI, Master)).thenReturn(Future.successful(false))
        when(clusterOperations.failoverMaster(redisURI)).thenReturn(Future.successful())

        val blackHole = TestActorRef(TestActors.blackholeProps)
        val failover = TestActorRef[Failover](Failover.props(blackHole))

        val command = FailoverMaster(redisURI)
        val msg = DoFailover(command, Slave)

        val attempts = FailoverConfigTest.maxNrAttemptsToVerify
        val delay = FailoverConfigTest.verificationRetryBackOffTime
        val expectedMessage = s"Failover incomplete, checking again in $delay seconds"

        EventFilter.warning(message = expectedMessage, occurrences = attempts) intercept {
          failover ! msg
        }
      }
    }

  }

}

object FailoverTest {
  val testSystem = {
    val config = ConfigFactory.parseString(
      """
         akka.loggers = [akka.testkit.TestEventListener]
      """)
    ActorSystem("FailoverTest", config)
  }
}

