package com.github.kliewkliew.cornucopia.actors

import akka.actor._
import akka.stream.actor.ActorPublisher

import scala.annotation.tailrec

/**
  * Copied liberally from akka documentaion on
  * [Stream integrations](http://doc.akka.io/docs/akka-stream-and-http-experimental/1.0/scala/stream-integrations.html).
  */
object CornucopiaSource {
  def props: Props = Props[CornucopiaSource]

  final case class Task(operation: String, redisNodeIp: String, ref: Option[ActorRef] = None)
  case object TaskAccepted
  case object TaskDenied
}

class CornucopiaSource extends ActorPublisher[CornucopiaSource.Task] {
  import CornucopiaSource._
  import akka.stream.actor.ActorPublisherMessage._

  val MaxBufferSize = 100
  var buf = Vector.empty[Task]

  override def receive = {
    case _: Task if buf.size == MaxBufferSize =>
      sender() ! TaskDenied
    case task: Task =>
      if (buf.isEmpty && totalDemand > 0) {
        val task2 = task.copy(ref = Some(sender))
        onNext(task2)
      }
      else {
        val task2 = task.copy(ref = Some(sender))
        buf :+= task2
        deliverBuf()
      }
    case Request(_) =>
      deliverBuf()
    case Cancel =>
      context.stop(self)
  }

  @tailrec final def deliverBuf(): Unit = {
    if (totalDemand > 0) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach(task => onNext(task))
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use foreach(task => onNext(task))
        deliverBuf()
      }
    }
  }

}
