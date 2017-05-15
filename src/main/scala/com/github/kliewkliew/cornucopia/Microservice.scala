package com.github.kliewkliew.cornucopia

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import com.github.kliewkliew.cornucopia.http.Server

object Microservice {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load().getConfig("cornucopia")
    val microserviceType = config.getString("microservice.type")
    val logger = LoggerFactory.getLogger(this.getClass)

    microserviceType match {
      case "kafka" =>
        logger.info("Using kafka as the microservice source")
        new graph.CornucopiaKafkaSource().run
      case "http" =>
        logger.info("Using http server as the microservice source")
        Server.start
    }
  }
}