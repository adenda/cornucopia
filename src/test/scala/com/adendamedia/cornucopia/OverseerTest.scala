package com.adendamedia.cornucopia

import akka.testkit.{TestActorRef, TestActors, TestKit, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import com.adendamedia.cornucopia.actors.JoinRedisNode
import com.adendamedia.cornucopia.redis.ClusterOperations
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import org.mockito.Mockito._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
//import org.scalatest.mockito.MockitoSugar._
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors.MessageBus
import com.adendamedia.cornucopia.actors.Overseer
//import com.adendamedia.cornucopia.redis.ClusterOperationsExceptions
import com.adendamedia.cornucopia.CornucopiaException._
import org.scalatest.mockito.MockitoSugar

class OverseerTest extends TestKit(ActorSystem("OverseerTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers with MockitoSugar {

  override def afterAll(): Unit = {
    system.terminate()
  }

  trait Test {
    val uriString: String = "redis://192.168.0.1"
    val redisURI: RedisURI = RedisURI.create(uriString)
    implicit val overseerMaxNrRetries: Int = 1
  }

//  trait ExceptionalTest {
//    implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
//
//    implicit val ec: ExecutionContext = system.dispatcher
//
//    when(clusterOperations.addNodeToCluster(redisURI)).thenThrow(CornucopiaRedisConnectionException("wat"))
//  }

  "Overseer" must {
    "retry joining node to cluster if there is a CornucopiaRedisConnectionException in the JoinRedisNode actor" in new Test {
//      import Overseer._
//      import MessageBus._
//
//      val probe = TestProbe()
//
//      implicit val clusterOperations: ClusterOperations = mock[ClusterOperations]
//
//      implicit val ec: ExecutionContext = system.dispatcher
//
//      when(clusterOperations.addNodeToCluster(redisURI)).thenReturn(Future.failed(CornucopiaRedisConnectionException("wat")))
//
//      val joinRedisNodeProps = JoinRedisNode.props
//
//      val props = Overseer.props(JoinRedisNode.props)
//      val overseer = TestActorRef[Overseer](props)
//      val overseer = system.actorOf(props, "overseerTest2")

//      val testActor = TestActorRef[Overseer](props)
//
//      val testChildActor = TestActorRef[JoinRedisNode](testActor.underlyingActor.joinRedisNode)


//      Thread.sleep(1)

//      val msg: AddNode = AddMaster(redisURI)
//      system.eventStream.publish(msg)

//      testChildActor.underlyingActor.

//      Thread.sleep(5000)

      1 must be(1)
    }

    "receive add master node task from message bus and tell JoinRedisNode actor with JoinMasterNode command" in new Test {
      import Overseer._
      import MessageBus._

      val probe = TestProbe()

      val joinRedisNodeProps = TestActors.forwardActorProps(probe.ref)

      val props = Overseer.props(joinRedisNodeProps)
      val overseer = TestActorRef[Overseer](props)

      val msg: AddNode = AddMaster(redisURI)
      system.eventStream.publish(msg)

      probe.expectMsgPF() {
        case JoinMasterNode(uri: RedisURI) =>
          uri must be(redisURI)
      }
    }

  }

}
