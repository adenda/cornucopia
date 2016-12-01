package com.github.kliewkliew.cornucopia.kafka

import akka.kafka.scaladsl.{Consumer => ConsumerDSL}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerRecord

class Consumer {
  type KafkaRecord = ConsumerRecord[Array[Byte], Array[Byte]]

  def run =
    ConsumerDSL.plainSource(Config.Consumer.settings, Config.Consumer.subscription)
      /*TODO*/
      .runWith(Sink.ignore)
}

// Operation message keys.
object Operations {
  val ADD_MASTER = "+master"
  val ADD_SLAVE = "+slave"
  val REMOVE_MASTER = "-master"
  val REMOVE_SLAVE = "-slave"
  val RESHARD = "*reshard"
}
