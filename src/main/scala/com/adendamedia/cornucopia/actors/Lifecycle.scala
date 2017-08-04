package com.adendamedia.cornucopia.actors

import akka.actor.{ActorRefFactory, ActorRef, Props, Actor, ActorLogging, OneForOneStrategy}
import akka.actor.SupervisorStrategy.Restart
import com.adendamedia.cornucopia.CornucopiaException.KillMeNow

object Lifecycle {
  def props(overseerMaker: ActorRefFactory => ActorRef): Props = Props(new Lifecycle(overseerMaker))

  val name = "lifecycle"
}

/**
  * This actor exists to be the parent of the overseer.
  */
class Lifecycle(overseerMaker: ActorRefFactory => ActorRef) extends Actor with ActorLogging {

  private val overseer =  overseerMaker(context)

  override def supervisorStrategy = OneForOneStrategy() {
    case KillMeNow(_) =>
      log.info(s"Restarting Overseer")
      Restart
  }

  override def receive: Receive = {
    case _ =>
      log.warning(s"That's strange")
  }
}
