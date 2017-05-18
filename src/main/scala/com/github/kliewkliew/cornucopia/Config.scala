package com.github.kliewkliew.cornucopia

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Producer, Consumer => ConsumerDSL}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.github.kliewkliew.cornucopia.actors.CornucopiaSource
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object Config {
  object Cornucopia {
    private val config = ConfigFactory.load().getConfig("cornucopia")
    val minReshardWait = config.getInt("reshard.interval").seconds
    val gracePeriod = config.getInt("grace.period") * 1000
    val refreshTimeout = config.getInt("refresh.timeout") * 1000
    val batchPeriod = config.getInt("batch.period").seconds
    val source = config.getString("source")
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

    val cornucopiaActorProps = CornucopiaSource.props
    val cornucopiaActorSource = Source.actorPublisher[CornucopiaSource.Task](cornucopiaActorProps)

    val cornucopiaKafkaSource = ConsumerDSL.plainSource(sourceSettings, subscription)

    val cornucopiaSource = ConsumerDSL.plainSource(sourceSettings, subscription)

    val cornucopiaSink = Producer.plainSink(sinkSettings)
  }

  object ReshardTableConfig {
    final implicit val ExpectedTotalNumberSlots: Int = 16384
  }

}
