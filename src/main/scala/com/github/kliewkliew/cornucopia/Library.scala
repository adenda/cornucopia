package com.github.kliewkliew.cornucopia

import akka.actor._

object Library {
  val ref: ActorRef = new graph.CornucopiaActorSource().ref
}
