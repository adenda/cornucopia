package com.adendamedia.cornucopia

import akka.actor.ActorSystem
import com.adendamedia.cornucopia.http.Server
import com.adendamedia.cornucopia.actors.SharedActorSystem
import org.slf4j.LoggerFactory

object Microservice {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)

    implicit val system: ActorSystem = SharedActorSystem.system

    // Start up Cornucopia
    val cornucopia = new Cornucopia

    logger.info("Starting http server")
    Server.start
  }
}