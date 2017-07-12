package com.adendamedia.cornucopia

import akka.testkit.{TestKit, TestProbe}
import akka.actor.{ActorSystem, ActorRef}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors.MessageBus._

class JoinRedisNodeTest extends TestKit(ActorSystem("JoinRedisNodeTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers {

  override def afterAll(): Unit = {
    system.terminate()
  }

  "JoinRedisNode" must {
    "add node to redis cluster" in {
    }
  }

}
