package com.adendamedia.cornucopia.actors

import com.lambdaworks.redis.RedisURI

/**
  * The message bus interface. This contains all the case class messages that can be published to and subscribed to on
  * the message bus.
  */
object MessageBus {

  trait AddNode {
    val uri: RedisURI
  }

  /**
    * Command to add a new master node to the Redis cluster with the given uri
    * @param uri The uri of the node to add
    */
  case class AddMaster(uri: RedisURI) extends AddNode

  /**
    * Command to add a new master node to the Redis cluster with the given uri
    * @param uri The uri of the node to add
    */
  case class AddSlave(uri: RedisURI) extends AddNode

  /**
    * Event indicating that a new node has been added to the Redis cluster with the given uri
    * @param uri The uri of the node that was added
    */
  case class NodeAdded(uri: RedisURI)

  /**
    * Event indicating that an attempt to add a redis node to the cluster has failed
    */
  trait FailedAddingRedisNode {
    val reason: String
  }

  /**
    * Event indicating that an attempt to add a redis node to the cluster has failed
    * @param reason Sentence explaining the problem
    */
  case class FailedAddingMasterRedisNode(reason: String) extends FailedAddingRedisNode

  /**
    * Signals to the actor hierarchy performing redis cluster commands that it should shutdown
    * @param message Optional message or reason for shutdown
    */
  case class Shutdown(message: Option[String] = None)
}
