package com.adendamedia.cornucopia.actors

import akka.actor.ActorSystem

object SharedActorSystem {
  lazy val system = ActorSystem()
}
