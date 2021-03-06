package com.adendamedia.cornucopia

import akka.testkit.{TestActorRef, TestKit, TestProbe}
import akka.actor.{ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}
import com.lambdaworks.redis.RedisURI
import com.adendamedia.cornucopia.actors.MessageBus._
import com.adendamedia.cornucopia.actors.Gatekeeper
import com.adendamedia.cornucopia.actors.Gatekeeper._

class GatekeeperTest extends TestKit(ActorSystem("GatekeeperTest"))
  with WordSpecLike with BeforeAndAfterAll with MustMatchers {

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Dispatcher" must {
    "publish task to add Master" in {
      val doTask = TestProbe()

      system.eventStream.subscribe(doTask.ref, classOf[AddMaster])

      val uriString: String = "redis://192.168.0.1"
      val redisURI: RedisURI = RedisURI.create(uriString)

      val submitTask = Task("+master", uriString)

      val gatekeeper: ActorRef = system.actorOf(Gatekeeper.props, "gatekeeper1")
      gatekeeper ! submitTask

      val msg = AddMaster(redisURI)

      doTask.expectMsg(msg)
    }

    "publish task to add Slave" in {
      val doTask = TestProbe()

      system.eventStream.subscribe(doTask.ref, classOf[AddSlave])

      val uriString: String = "redis://192.168.0.1"
      val redisURI: RedisURI = RedisURI.create(uriString)

      val submitTask = Task("+slave", uriString)

      val gatekeeper: ActorRef = system.actorOf(Gatekeeper.props, "gatekeeper2")
      gatekeeper ! submitTask

      val msg = AddSlave(redisURI)

      doTask.expectMsg(msg)
    }

    "publish task to remove Master" in {
      val doTask = TestProbe()

      system.eventStream.subscribe(doTask.ref, classOf[RemoveMaster])

      val uriString: String = "redis://192.168.0.1"
      val redisURI: RedisURI = RedisURI.create(uriString)

      val submitTask = Task("-master", uriString)

      val gatekeeper: ActorRef = TestActorRef[Gatekeeper](Gatekeeper.props)
      gatekeeper ! submitTask

      val msg = RemoveMaster(redisURI)

      doTask.expectMsg(msg)
    }

  }

}
