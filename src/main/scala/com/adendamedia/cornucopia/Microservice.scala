package com.adendamedia.cornucopia

import com.adendamedia.cornucopia.http.Server
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Microservice {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load().getConfig("cornucopia")
    val logger = LoggerFactory.getLogger(this.getClass)

    logger.info("Starting http server")
    Server.start
  }
}