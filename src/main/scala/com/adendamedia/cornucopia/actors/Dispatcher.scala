package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.redis._
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.adendamedia.cornucopia.actors.Gatekeeper.{TaskAccepted, TaskDenied}
import com.lambdaworks.redis.RedisURI

/**
  * The dispatcher publishes redis cluster commands to be performed, and waits for them to be completed. The
  * dispatcher subscribes to events on the message bus indicating when an operation has completed successfully or
  * unsuccessfully. The dispatcher is completely decoupled from the actor hierarchy performing Redis cluster operations.
  * The dispatcher can process a Redis command one at a time. When processing a Redis command, it uses `become` to
  * change its state to `running`. When the dispatcher is running it will no longer accept other Redis cluster commands
  * to publish. When it receives a success message on the message bus, the dispatcher will change its state back to
  * `accepting`, and it will message the client ActorRef of the success.
  */
object Dispatcher {
  def props: Props = Props(new Dispatcher)

  case class DispatchInformation(task: DispatchTask, ref: ActorRef)

  case class DispatchTask(operation: Operation, redisURI: RedisURI)

  case object CheckTimeout

  val name = "dispatcher"
}

class Dispatcher extends Actor with ActorLogging {
  import Dispatcher._
  import MessageBus._

  context.system.eventStream.subscribe(self, classOf[NodeAdded])
  context.system.eventStream.subscribe(self, classOf[NodeRemoved])
  context.system.eventStream.subscribe(self, classOf[FailedCornucopiaCommand])

  def receive: Receive = accepting

  private def accepting: Receive = {
    case task: DispatchTask =>
      publishTask(task.operation, task.redisURI)
      val dispatchInformation = DispatchInformation(task, sender)
      sender ! TaskAccepted
      context.become(dispatching(dispatchInformation))
  }

  private def dispatching(dispatchInformation: DispatchInformation): Receive = {
    case task: DispatchTask =>
      val forbiddenTask = s"${task.operation.message} ${task.redisURI}"
      val currentTask = s"${dispatchInformation.task.operation.message} ${dispatchInformation.task.redisURI}"
      log.warning(s"Cannot dispatch task '$forbiddenTask' while waiting on task '$currentTask'")
      sender ! TaskDenied
    case event: NodeAdded =>
      log.info(s"Redis node '${event.uri}' successfully added to cluster")
      val ref = dispatchInformation.ref
      ref ! Right((dispatchInformation.task.operation.key, event.uri))
      context.unbecome()
    case event: NodeRemoved =>
      log.info(s"Redis node '${event.uri}' successfully removed")
      val ref = dispatchInformation.ref
      ref ! Right((dispatchInformation.task.operation.key, event.uri))
      context.unbecome()
    case event: FailedAddingMasterRedisNode =>
      log.warning(s"Redis node ${event.uri} failed being added to cluster")
      val ref = dispatchInformation.ref
      ref ! Left((dispatchInformation.task.operation.key, event.uri))
      context.unbecome()
    case event: FailedRemovingMasterRedisNode =>
      log.warning(s"Redis node ${event.uri} failed being removed from cluster")
      val ref = dispatchInformation.ref
      ref ! Left((dispatchInformation.task.operation.key, event.uri))
      context.unbecome()
  }

  private def publishTask(operation: Operation, redisURI: RedisURI) = operation match {
    case ADD_MASTER =>
      val msg = AddMaster(redisURI)
      context.system.eventStream.publish(msg)
    case ADD_SLAVE =>
      val msg = AddSlave(redisURI)
      context.system.eventStream.publish(msg)
    case REMOVE_MASTER       =>
      val msg = RemoveMaster(redisURI)
      context.system.eventStream.publish(msg)
    case REMOVE_SLAVE        =>
      val msg = RemoveSlave(redisURI)
      context.system.eventStream.publish(msg)
  }

}
