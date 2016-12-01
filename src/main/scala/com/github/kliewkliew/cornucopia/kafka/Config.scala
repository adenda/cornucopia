package com.github.kliewkliew.cornucopia.kafka

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

object Config {
  object Cornucopia {
    private val cornucopiaConfig = ConfigFactory.load().getConfig("cornucopia")
    val minReshardWait = cornucopiaConfig.getInt("reshard.interval").seconds
  }

  object Consumer {
    private val kafkaConfig = ConfigFactory.load().getConfig("kafka")
    private val kafkaServers = kafkaConfig.getString("bootstrap.servers")
    private val kafkaConsumerConfig = kafkaConfig.getConfig("consumer")
    private val topic = kafkaConsumerConfig.getString("topic")
    private val groupId = kafkaConsumerConfig.getString("group.id")

    implicit private val actorSystem = ActorSystem()
    // Log failures and resume processing
    private val decider: Supervision.Decider = { e =>
      LoggerFactory.getLogger(this.getClass).error("Failed to process event", e)
      Supervision.Resume
    }

    private val materializerSettings =
    ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)
    implicit private val materializer = ActorMaterializer(materializerSettings)(actorSystem)

    val settings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaServers)
      .withGroupId(groupId)
    val subscription = Subscriptions.topics(topic)
  }

}
