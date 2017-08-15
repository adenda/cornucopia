package com.adendamedia.cornucopia.actors

import akka.actor.{Actor, ActorLogging, ActorRef}
import Overseer._

/**
  * Behaviour that the top-level actor of a processing stage must implement
  */
trait CornucopiaSupervisor[C <: OverseerCommand] extends Actor with ActorLogging {

  protected def accepting: Receive

  protected def processing[D <: C](command: D, ref: ActorRef): Receive
}
