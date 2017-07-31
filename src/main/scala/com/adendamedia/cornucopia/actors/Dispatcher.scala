package com.adendamedia.cornucopia.actors

import com.adendamedia.cornucopia.redis._
import com.adendamedia.cornucopia.Config._

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import scala.concurrent.duration._
import com.lambdaworks.redis.RedisURI

/**
  * The dispatcher publishes redis cluster commands to be performed, and waits for them to be completed. The
  * dispatcher subscribes to events on the message bus indicating when an operation has completed successfully or
  * unsuccessfully. The dispatcher is completely decoupled from the actor hierarchy performing Redis cluster operations.
  * The dispatcher can process a Redis command one at a time. When processing a Redis command, it uses `become` to
  * change its state to `running`. When the dispatcher is running it will no longer accept other Redis cluster commands
  * to publish. The dispatcher schedules itself using a timeout period. When this timeout period is reached it will send
  * a message to itself. If it has not completed processing a command when the timeout occurs, it will publish a message
  * to the message bus to shutdown, and it will send a message back to the client ActorRef that the command has failed.
  * When it receives a success message on the message bus, the dispatcher will change its state back to `accepting`, and
  * it will message the client ActorRef of the success.
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

  import context.dispatcher

  context.system.eventStream.subscribe(self, classOf[NodeAdded])
  context.system.eventStream.subscribe(self, classOf[NodeRemoved])

  def receive: Receive = accepting

  private def accepting: Receive = {
    case task: DispatchTask =>
      publishTask(task.operation, task.redisURI)
//      context.system.scheduler.scheduleOnce(dispatchTaskTimeout.seconds, self, CheckTimeout)
      val dispatchInformation = DispatchInformation(task, sender)
      context.become(dispatching(dispatchInformation))
    case event: NodeAdded =>
      log.error(s"Dispatcher received event NodeAdded, but is currently accepting new operations")
    case CheckTimeout =>
  }

  private def dispatching(dispatchInformation: DispatchInformation): Receive = {
    case task: DispatchTask =>
      val forbiddenTask = s"${task.operation.message} ${task.redisURI}"
      val currentTask = s"${dispatchInformation.task.operation.message} ${dispatchInformation.task.redisURI}"
      log.warning(s"Cannot dispatch task '$forbiddenTask' while waiting on task '$currentTask'")
    case event: NodeAdded =>
      log.info(s"Redis node '${event.uri}' successfully added to cluster")
      val ref = dispatchInformation.ref
      ref ! Right((dispatchInformation.task.operation.key, event.uri)) // TODO: Make this better
      context.unbecome()
    case event: NodeRemoved =>
      log.info(s"Redis node '${event.uri}' successfully removed")
      val ref = dispatchInformation.ref
      ref ! Right((dispatchInformation.task.operation.key, event.uri)) // TODO: Make this better
      context.unbecome()
    case CheckTimeout =>
      log.error(s"Dispatch task has failed with timeout after $dispatchTaskTimeout seconds.")
      val ref = dispatchInformation.ref
      ref ! Left((dispatchInformation.task.operation.key, dispatchInformation.task.redisURI, "Timeout"))
      context.system.eventStream.publish(Shutdown)
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
    case CLUSTER_TOPOLOGY    =>
    case _                   =>
  }

}
