package com.github.kliewkliew.cornucopia

import actors.CornucopiaSource
import akka.actor._
import redis.Connection.{newSaladAPI, Salad}

object Library {
  implicit val newSaladAPIimpl: Salad = newSaladAPI
  val ref: ActorRef = new graph.CornucopiaActorSource().ref
  val source = CornucopiaSource
}
