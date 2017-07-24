package com.adendamedia.cornucopia.actors

import akka.actor.{Actor, ActorLogging, ActorRef}

/**
  * Behaviour that the top-level actor of a processing stage must implement
  */
trait CornucopiaSupervisor extends Actor with ActorLogging {
  import Overseer._

  protected def accepting: Receive

  protected def processing(command: OverseerCommand, ref: ActorRef): Receive
}
