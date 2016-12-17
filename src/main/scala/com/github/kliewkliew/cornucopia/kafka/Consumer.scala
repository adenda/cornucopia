package com.github.kliewkliew.cornucopia.kafka

import Config.Consumer.{cornucopiaSource, materializer}
import com.github.kliewkliew.cornucopia.redis.Connection._
import com.github.kliewkliew.salad.api.async.FutureConverters._
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Flow, GraphDSL, Partition, RunnableGraph, Sink}
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Consumer {
  type Record = ConsumerRecord[String, String]
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Run the graph to process the event stream from Kafka.
    * @return
    */
  def run = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val in = cornucopiaSource
    val out = Sink.ignore

    def partitionEvents(key: String) = key match {
      case ADD_MASTER.key     => ADD_MASTER.ordinal
      case ADD_SLAVE.key      => ADD_SLAVE.ordinal
      case REMOVE_MASTER.key  => REMOVE_MASTER.ordinal
      case REMOVE_SLAVE.key   => REMOVE_SLAVE.ordinal
      case RESHARD.key        => RESHARD.ordinal
      case _                  => UNSUPPORTED.ordinal
    }

    val eventPartition = builder.add(Partition[Record](
      6, { record => partitionEvents(record.key)}))

    in ~> eventPartition
          eventPartition.out(ADD_MASTER.ordinal)    ~> streamAddMaster      ~> out
          eventPartition.out(ADD_SLAVE.ordinal)     ~> streamAddSlave       ~> out
          eventPartition.out(REMOVE_MASTER.ordinal) ~> streamRemoveMaster   ~> out
          eventPartition.out(REMOVE_SLAVE.ordinal)  ~> streamRemoveSlave    ~> out
          eventPartition.out(RESHARD.ordinal)       ~> streamReshard        ~> out
          eventPartition.out(UNSUPPORTED.ordinal)   ~> unsupportedOperation ~> out

    ClosedShape
  }).run()

  /**
    * TODO:
    * - batch requests
    * - clear master queue before slave queue
    * - resharding limited per interval (min wait period)
    * */

  /**
    * Stream definitions for the graph.
    */
  private def streamAddMaster = Flow[Record]
    .map(_.value)
    .map(new URI(_))
    .map(addMaster)
  private def streamAddSlave = Flow[Record]
    .map(_.value)
    .map(new URI(_))
    .map(addSlave)
  private def streamRemoveMaster = Flow[Record]
    .map(_.value)
    .map(new URI(_))
    .map(removeMaster)
  private def streamRemoveSlave = Flow[Record]
    .map(_.value)
    .map(new URI(_))
    .map(removeSlave)
  private def streamReshard = Flow[Record]
    .map(_ => reshardCluster)
  private def unsupportedOperation = Flow[Record]
    .map(record => throw new IllegalArgumentException(s"Unsupported operation ${record.key} for ${record.value}"))

  /**
    * Add a master node to the cluster and redistribute the hash slots of the cluster.
    * @param uRI The URI of the master that will be added to the cluster.
    * @return Indicate that the master was added to the cluster and the hash slots were redistributed.
    */
  private def addMaster(uRI: URI): Future[Boolean] =
    saladAPI.underlying.clusterMeet(uRI.getHost, uRI.getPort).isOK
    .flatMap(ok => reshardCluster)

  /**
    * Add a slave node to the cluster, replicating the master that has the fewest slaves.
    * @param uRI The URI of the slave that will be added to the cluster.
    * @return Indicate that the slave was added to the cluster.
    */
  // TODO: batch slave requests and use max-heap to find the poorest n masters for n slaves
  private def addSlave(uRI: URI): Future[Boolean] =
    clusterNodes.flatMap { allNodes =>
      // Map of master node ids to the number of slaves for that master.
      val masterSlaveCount = new ConcurrentHashMap[String, AtomicInteger](
        allNodes.size,
        0.75f,
        Runtime.getRuntime.availableProcessors
      )
      // Populate the map.
      allNodes.map { node =>
        masterSlaveCount.get(node.getSlaveOf).incrementAndGet()
      }

      val poorestMaster = masterSlaveCount.asScala
        .map(kv => (kv._1, kv._2.intValue))
        .reduce { (A, B) =>
          if (A._2 < B._2)
            A
          else
            B
        }._1

      connection(uRI).clusterReplicate(poorestMaster).isOK
    }

  /**
    * Safely remove a master by redistributing its hash slots before blacklisting it from the cluster.
    * The data is given time to migrate as configured in `cornucopia.grace.period`.
    * @param uRI The URI of the master that will be removed from the cluster.
    * @return Indicate that the hash slots were redistributed and the master removed from the cluster.
    */
  private def removeMaster(uRI: URI): Future[Boolean] =
    masterNodes
      .map(allNodes => allNodes.filterNot(node(uRI).getNodeId == _.getNodeId))
      .flatMap(viewWithoutThisNode => reshardCluster(viewWithoutThisNode))
      .map(_ => scala.concurrent.blocking { Thread.sleep(Config.Cornucopia.gracePeriod) }) // Allow data to migrate
      .flatMap(_ => removeNode(uRI))

  /**
    * Remove a slave node from the cluster.
    * @param uRI The URI of the slave that will be removed from the cluster.
    * @return Indicate that the slave was removed from the cluster.
    */
  private def removeSlave(uRI: URI): Future[Boolean] =
    removeNode(uRI)

  private def removeNode(uRI: URI): Future[Boolean] =
    saladAPI.underlying.clusterForget(node(uRI).getNodeId).isOK

  /**
    * Reshard the cluster using the current cluster view.
    * @return Boolean indicating that all hash slots were reassigned successfully.
    */
  private def reshardCluster: Future[Boolean] = masterNodes.flatMap(reshardCluster)

  /**
    * Reshard the cluster using a view of the cluster consisting of a subset of master nodes.
    * @param masters The master nodes that will be assigned hash slots.
    * @return Boolean indicating that all hash slots were reassigned successfully.
    */
  // TODO: implement this as a batched stage in the stream so that it doesn't need to be synchronized
  // batch node additions and removals before constructing the final view to pass to this function
  private def reshardCluster(masters: mutable.Buffer[RedisClusterNode]): Future[Boolean] = synchronized {
    val reshardResults = List.range(1, 16384).map { slot =>
      saladAPI.underlying.clusterSetSlotNode(slot, masters(slot % masters.length).getNodeId).isOK
    }

    Future.sequence(reshardResults).map(_.forall(_ == true))
  }

}

