package com.github.kliewkliew.cornucopia.actors

import akka.actor.ActorSystem

object SharedActorSystem {
  val sharedActorSystem = ActorSystem()
}
