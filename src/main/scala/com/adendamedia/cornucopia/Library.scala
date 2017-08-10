package com.adendamedia.cornucopia

import akka.actor._
import com.adendamedia.cornucopia.actors.Gatekeeper

trait CornucopiaLibrary {
  val cornucopia: Cornucopia
  val ref: ActorRef
}

class Library(implicit val system: ActorSystem) extends CornucopiaLibrary {
  /**
    * Start up Cornucopia
    */
  val cornucopia = new Cornucopia

  /**
    * Use this actorRef in your applications to send command messages to Cornucopia
    */
  val ref: ActorRef = system.actorOf(Gatekeeper.props, Gatekeeper.name)
}
