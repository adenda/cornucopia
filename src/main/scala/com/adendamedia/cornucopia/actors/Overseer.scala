package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import com.adendamedia.cornucopia.CornucopiaException._

import com.lambdaworks.redis.RedisURI

object Overseer {
  def props(joinRedisNodeProps: Props)(implicit overseerMaxNrRetries: Int): Props =
    Props(new Overseer(joinRedisNodeProps))

  trait OverseerCommand

  trait NodeAddedEvent {
    val uri: RedisURI
  }

  trait JoinNode extends OverseerCommand {
    val redisURI: RedisURI
  }

  case class JoinMasterNode(redisURI: RedisURI) extends JoinNode
  case class JoinSlaveNode(redisURI: RedisURI) extends JoinNode

  case class MasterNodeAdded(uri: RedisURI) extends NodeAddedEvent
  case class SlaveNodeAdded(uri: RedisURI) extends NodeAddedEvent
}

/**
  * The overseer subscribes to Redis commands that have been published by the dispatcher. This actor is the parent
  * actor of all actors that process Redis cluster commands. Cluster commands include adding and removing nodes. The
  * overseer subscribes to the Shutdown message, whereby after receiving this message, it will restart its children.
  */
class Overseer(joinRedisNodeProps: Props)(implicit overseerMaxNrRetries: Int) extends Actor with ActorLogging {
  import MessageBus.{AddNode, AddMaster, AddSlave, Shutdown}
  import Overseer._

  val joinRedisNode: ActorRef = context.actorOf(joinRedisNodeProps, "joinRedisNode")

  context.system.eventStream.subscribe(self, classOf[AddNode])
  context.system.eventStream.subscribe(self, classOf[Shutdown])

  override def supervisorStrategy = OneForOneStrategy(overseerMaxNrRetries) {
    case e: FailedOverseerCommand => Restart
  }

  override def receive: Receive = {
    case m: AddMaster =>
      log.debug(s"Received message AddMaster(${m.uri})")
      addMaster(m.uri)
    case masterNodeAdded: MasterNodeAdded => // TODO
    case slaveNodeAdded: SlaveNodeAdded => // TODO
  }

  private def addMaster(uri: RedisURI) = {
    joinRedisNode ! JoinMasterNode(uri)
  }

}
