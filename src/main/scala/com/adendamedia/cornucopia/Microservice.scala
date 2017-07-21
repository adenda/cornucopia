package com.adendamedia.cornucopia

import com.adendamedia.cornucopia.http.Server
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Microservice {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)

    // Start up Cornucopia
    val cornucopia = new Cornucopia

    logger.info("Starting http server")
    Server.start
  }
}