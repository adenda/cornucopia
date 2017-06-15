package com.adendamedia.cornucopia.actors

import akka.actor.ActorSystem

object SharedActorSystem {
  val sharedActorSystem = ActorSystem()
}
