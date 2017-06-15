package com.adendamedia.cornucopia

import actors.CornucopiaSource
import akka.actor._

object Library {

  /**
    * Use this actorRef in your applications to send command messages to Cornucopia
    */
  val ref: ActorRef = new graph.CornucopiaActorSource().ref

  /**
    * Include this object to send Task commands to Cornucopia ActorRef above
    */
  val source = CornucopiaSource
}
