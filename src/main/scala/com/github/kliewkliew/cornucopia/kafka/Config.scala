package com.github.kliewkliew.cornucopia.kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Producer, Consumer => ConsumerDSL}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object Config {
  object Cornucopia {
    private val cornucopiaConfig = ConfigFactory.load().getConfig("cornucopia")
    val minReshardWait = cornucopiaConfig.getInt("reshard.interval").seconds
    val gracePeriod = cornucopiaConfig.getInt("grace.period")
    val refreshTimeout = cornucopiaConfig.getInt("refresh.timeout") * 1000
  }

  object Consumer {
    private val kafkaConfig = ConfigFactory.load().getConfig("kafka")
    private val kafkaServers = kafkaConfig.getString("bootstrap.servers")
    private val kafkaConsumerConfig = kafkaConfig.getConfig("consumer")
    private val topic = kafkaConsumerConfig.getString("topic")
    private val groupId = kafkaConsumerConfig.getString("group.id")

    implicit val actorSystem = ActorSystem()
    // Log failures and resume processing
    private val decider: Supervision.Decider = { e =>
      LoggerFactory.getLogger(this.getClass).error("Failed to process event", e)
      Supervision.Resume
    }
    private val materializerSettings = ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)

    private val sourceSettings = ConsumerSettings(actorSystem, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(kafkaServers)
      .withGroupId(groupId)
    private val subscription = Subscriptions.topics(topic)

    private val sinkSettings = ProducerSettings(actorSystem, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaServers)

    implicit val materializer = ActorMaterializer(materializerSettings)(actorSystem)
    val cornucopiaSource = ConsumerDSL.plainSource(sourceSettings, subscription)
    val cornucopiaSink = Producer.plainSink(sinkSettings)
  }

}
