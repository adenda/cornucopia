package com.github.kliewkliew.cornucopia.kafka


import Config.Consumer.materializer
import Operations._

import com.github.kliewkliew.cornucopia.redis.Connection.saladAPI
import com.github.kliewkliew.salad.api.FutureConverters._

import akka.kafka.scaladsl.{Consumer => ConsumerDSL}
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, Partition, RunnableGraph, Sink}
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.{ClusterPartitionParser, RedisClusterNode}
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import collection.JavaConverters._
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Consumer {
  type KafkaRecord = ConsumerRecord[String, String]

  def run = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val in = ConsumerDSL.plainSource(Config.Consumer.settings, Config.Consumer.subscription)
    val out = Sink.ignore

    val eventPartition = builder.add(Partition[KafkaRecord](5, { _.key() match {
      case ADD_MASTER => 0
      case ADD_SLAVE => 1
      case REMOVE_MASTER => 2
      case REMOVE_SLAVE => 3
      case _ => 4
    }

    }))
    val logMerge = builder.add(Merge[String](2))

    in ~> eventPartition
    eventPartition.out(0) ~> streamAddMaster ~>  out
    eventPartition.out(1) ~> streamAddSlave ~> out
    eventPartition.out(2) ~> streamRemoveMaster ~> out
    eventPartition.out(3) ~> streamRemoveSlave ~> out
    eventPartition.out(4) ~> unsupportedOperation ~> out

    ClosedShape
  }).run()

  /**
    * TODO:
    * - batch requests
    * - clear master queue before slave queue
    * - limit the maximum wait time for all queues
    * - resharding limited per interval (min wait period) and can interrupt master/slave queue
    * */


  def streamAddMaster = Flow[KafkaRecord]
    .map(_.value)
    .map(new URI(_))
    .map(addMaster)

  def streamAddSlave = Flow[KafkaRecord]
    .map(_.value)
    .map(new URI(_))
    .map(addSlave)

  def streamRemoveMaster = Flow[KafkaRecord]
    .map(_.value)
    .map(new URI(_))
    .map(removeMaster)

  def streamRemoveSlave = Flow[KafkaRecord]
    .map(_.value)
    .map(new URI(_))
    .map(removeSlave)

  def unsupportedOperation = Flow[KafkaRecord]
    .map(record => throw new IllegalArgumentException(s"Unsupported operation ${record.key} for ${record.value}"))

  def addMaster(uRI: URI): Future[Boolean] =
    saladAPI.underlying.clusterMeet(uRI.getHost, uRI.getPort).isOK

  // TODO: batch slave requests and use min-heap to find the poorest n masters for n slaves
  def addSlave(uRI: URI): Future[Boolean] = {
    saladAPI.underlying.clusterNodes.flatMap { response =>
      val allNodes = ClusterPartitionParser.parse(response).getPartitions.asScala

      val masterSlaveCount = new ConcurrentHashMap[String, AtomicInteger](
        allNodes.size,
        0.75f,
        Runtime.getRuntime.availableProcessors
      )

      allNodes.map { node =>
        masterSlaveCount.get(node.getSlaveOf).incrementAndGet()
      }

      val poorestMaster = masterSlaveCount.asScala.map(kv => (kv._1, kv._2.intValue)).reduce { (A, B) =>
        if (A._2 < B._2)
          A
        else
          B
      }._1

      saladAPI.underlying.clusterReplicate(poorestMaster).isOK
    }
  }

  def removeMaster(uRI: URI): Future[Boolean] = {
    removeNode(uRI)
    reshardCluster
  }

  def removeSlave(uRI: URI): Future[Boolean] = removeNode(uRI)

  def removeNode(uRI: URI): Future[Boolean] = {
    val node: RedisClusterNode = new RedisClusterNode
    node.setUri(RedisURI.create(uRI.getHost, uRI.getPort))
    saladAPI.underlying.clusterForget(node.getNodeId).isOK
  }

  def reshardCluster = Future(false) // TODO

}

// Operation message keys.
object Operations {
  val ADD_MASTER = "+master"
  val ADD_SLAVE = "+slave"
  val REMOVE_MASTER = "-master"
  val REMOVE_SLAVE = "-slave"
}
