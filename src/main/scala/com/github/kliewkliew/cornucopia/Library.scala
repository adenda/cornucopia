package com.github.kliewkliew.cornucopia

import actors.CornucopiaSource
import akka.actor._

object Library {
  val ref: ActorRef = new graph.CornucopiaActorSource().ref
  val source = CornucopiaSource
}
