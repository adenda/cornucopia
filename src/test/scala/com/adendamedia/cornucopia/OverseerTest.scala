package com.adendamedia.cornucopia

import akka.testkit.{TestKit, TestProbe, TestActors}
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.actors.MessageBus._

class OverseerTest extends TestKit(ActorSystem("OverseerTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers {

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Overseer" must {
    "receive add master node task and send message to JoinRedisNode actor" in {
      import Overseer._
      import MessageBus._

      val joinRedisNodeProps = TestActors.forwardActorProps(testActor)

      val props = Overseer.props(joinRedisNodeProps)
      val overseer = system.actorOf(props, "overseerTest")

      val uriString: String = "redis://192.168.0.1"
      val redisURI: RedisURI = RedisURI.create(uriString)

      system.eventStream.publish(AddMaster(redisURI))

      expectMsgPF() {
        case JoinMasterNode(uri: RedisURI) =>
          uri must be(redisURI)
      }
    }
  }

}
