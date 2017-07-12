package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.redis._
import com.adendamedia.cornucopia.Config._

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import scala.concurrent.duration._
import com.lambdaworks.redis.RedisURI

object Overseer {
  def props(joinRedisNodeProps: Props): Props = Props(new Overseer(joinRedisNodeProps))

  case class JoinMasterNode(redisURI: RedisURI)
}

/**
  * The overseer subscribes to Redis commands that have been published by the dispatcher. This actor is the parent
  * actor of all actors that process Redis cluster commands. Cluster commands include adding and removing nodes. The
  * overseer subscribes to the Shutdown message, whereby after receiving this message, it will restart its children.
  */
class Overseer(joinRedisNodeProps: Props) extends Actor with ActorLogging {
  import MessageBus.{AddNode, AddMaster, AddSlave, Shutdown}
  import Overseer._

  context.system.eventStream.subscribe(self, classOf[AddNode])
  context.system.eventStream.subscribe(self, classOf[Shutdown])

  private val joinRedisNode: ActorRef = context.actorOf(joinRedisNodeProps, "joinRedisNode")

  override def receive: Receive = {
    case m: AddMaster => addMaster(m.uri)
  }

  private def addMaster(uri: RedisURI) = {
    joinRedisNode ! JoinMasterNode(uri)
  }

}
