package com.github.kliewkliew.cornucopia.kafka

import Config.Consumer.{cornucopiaSource, materializer}
import com.github.kliewkliew.cornucopia.redis.Connection._
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Flow, GraphDSL, Partition, RunnableGraph, Sink}
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.models.role.RedisInstance.Role
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
    *
    * @return
    */
  def run = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val in = cornucopiaSource
    val out = Sink.ignore

    def partitionEvents(key: String) = key.trim match {
      case ADD_MASTER.key     => ADD_MASTER.ordinal
      case ADD_SLAVE.key      => ADD_SLAVE.ordinal
      case REMOVE_NODE.key    => REMOVE_NODE.ordinal
      case RESHARD.key        => RESHARD.ordinal
      case _                  => UNSUPPORTED.ordinal
    }

    val eventPartition = builder.add(Partition[Record](
      5,  record => partitionEvents(record.key)))

    in ~> eventPartition
          eventPartition.out(ADD_MASTER.ordinal)    ~> streamAddMaster      ~> out
          eventPartition.out(ADD_SLAVE.ordinal)     ~> streamAddSlave       ~> out
          eventPartition.out(REMOVE_NODE.ordinal)   ~> streamRemoveNode     ~> out
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
  // Add a master node to the cluster and redistribute the hash slots to the cluster.
  private def streamAddMaster = Flow[Record]
    .map(_.value)
    .map(RedisURI.create)
    .mapAsync(1)(addNodeToCluster)
    .mapAsync(1)(waitForTopologyRefresh)
    .mapAsync(1)(_ => reshardCluster)
    .mapAsync(1)(waitForTopologyRefresh)
  // Add a slave node to the cluster, replicating the master that has the fewest slaves.
  private def streamAddSlave = Flow[Record]
    .map(_.value)
    .map(RedisURI.create)
    .mapAsync(1)(addNodeToCluster)
    .mapAsync(1)(waitForTopologyRefresh)
    .mapAsync(1)(findMaster)
    .mapAsync(1)(waitForTopologyRefresh)
  // Safely remove a master by redistributing its hash slots before blacklisting it from the cluster.
  // The data is given time to migrate as configured in `cornucopia.grace.period`.
  // Immediately remove a slave node from the cluster.
  private def streamRemoveNode = Flow[Record]
    .map(_.value)
    .map(RedisURI.create)
    .mapAsync(1)(removeNode)
    .mapAsync(1)(waitForTopologyRefresh)
  // Redistribute the hash slots among all nodes in the cluster.
  private def streamReshard = Flow[Record]
    .mapAsync(1)(_ => reshardCluster)
    .mapAsync(1)(waitForTopologyRefresh)
  // Throw for keys indicating unsupported operations.
  private def unsupportedOperation = Flow[Record]
    .map(record => throw new IllegalArgumentException(s"Unsupported operation ${record.key} for ${record.value}"))

  private def addNodeToCluster(uRI: RedisURI): Future[RedisURI] =
    newSaladAPI.clusterMeet(uRI).map(_ => uRI)
  private def waitForTopologyRefresh[T](passthrough: T): Future[T]  = Future {
    scala.concurrent.blocking(Thread.sleep(Config.Cornucopia.refreshTimeout))
    passthrough
  }

  /**
    * Set the slave node to replicate the master that has the fewest slaves.
    *
    * @param redisURI The URI of the slave that will be added to the cluster.
    * @return Indicate that the slave was added to the cluster.
    */
  // TODO: batch slave requests and use max-heap to find the poorest n masters for n slaves
  private def findMaster(redisURI: RedisURI): Future[Unit] = {
    implicit val saladAPI = newSaladAPI
    val slaveConnection = getConnection(redisURI)
    val getSlaveId = slaveConnection.clusterMyId
    val getAllNodes = saladAPI.clusterNodes

    for {
      slaveId <- getSlaveId
      allNodes <- getAllNodes
    } yield {
      // Map of master node ids to the number of slaves for that master.
      val masterSlaveCount = new ConcurrentHashMap[String, AtomicInteger](
        allNodes.size + 1,
        1,
        Runtime.getRuntime.availableProcessors
      )
      // Populate the map.
      saladAPI.masterNodes(allNodes)
        .filterNot(slaveId == _.getNodeId)
        .foreach(master => masterSlaveCount.put(master.getNodeId, new AtomicInteger(0)))
      allNodes.map { node =>
        Option.apply(node.getSlaveOf)
          .map(master => masterSlaveCount.get(master).incrementAndGet())
      }

      val poorestMaster = masterSlaveCount.asScala
        .map(kv => (kv._1, kv._2.intValue))
        .reduce { (A, B) =>
          if (A._2 < B._2)
            A
          else
            B
        }._1

      slaveConnection.clusterReplicate(poorestMaster)
    }
  }

  def removeNode(redisURI: RedisURI) = {
    implicit val saladAPI = newSaladAPI
    val getRemovalId = getConnection(redisURI).clusterMyId
    val getAllNodes = saladAPI.clusterNodes

    for {
      removalId <- getRemovalId
      allNodes <- getAllNodes
    } yield {
      val role = allNodes.find(removalId == _.getNodeId).get.getRole
      role match {
        case Role.MASTER => removeMaster(removalId)
        case Role.SLAVE  => forgetNode(removalId)
        case nodeType    => throw new Exception(s"$nodeType not supported")
      }
    }
  }

  /**
    * Safely remove a master by redistributing its hash slots before blacklisting it from the cluster.
    * The data is given time to migrate as configured in `cornucopia.grace.period`.
    *
    * @param removalId The node id of the master that will be removed from the cluster.
    * @return Indicate that the hash slots were redistributed and the master removed from the cluster.
    */
  private def removeMaster(removalId: String)(implicit saladAPI: SaladAPI): Future[Unit] =
    saladAPI.clusterNodes.map { allNodes =>
      val masterViewWithoutThisNode = saladAPI.masterNodes(allNodes).filterNot(removalId == _.getNodeId)
      val reshardDone = reshardCluster(masterViewWithoutThisNode).map { _ =>
        scala.concurrent.blocking(Thread.sleep(Config.Cornucopia.gracePeriod)) // Allow data to migrate
      }
      reshardDone.map(_ => forgetNode(removalId))
    }

  /**
    * Notify all nodes in the cluster to forget this node.
    *
    * @param removalId The node id of the node to be forgotten by the cluster.
    * @return
    */
  def forgetNode(removalId: String)(implicit saladAPI: SaladAPI): Future[Unit] =
    saladAPI.clusterNodes.flatMap { allNodes =>
      val connectionToRemovalNode = getConnection(removalId)
      val opNodes = allNodes
        .filterNot(removalId == _.getNodeId) // Node cannot forget itself
        .filterNot(removalId == _.getSlaveOf) // Node cannot forget its master
      val listFutureResults = opNodes.map { node =>
        getConnection(node.getNodeId).clusterForget(removalId)
      }
      Future.sequence(listFutureResults)
        .flatMap(_ => connectionToRemovalNode.clusterReset(true))
    }

  /**
    * Reshard the cluster using the current cluster view.
    *
    * @return Boolean indicating that all hash slots were reassigned successfully.
    */
  private def reshardCluster: Future[Unit] = {
    implicit val saladAPI = newSaladAPI
    saladAPI.masterNodes.flatMap(reshardCluster)
  }

  /**
    * Reshard the cluster using a view of the cluster consisting of a subset of master nodes.
    *
    * @param masters The master nodes that will be assigned hash slots.
    * @return Boolean indicating that all hash slots were reassigned successfully.
    */
  // TODO: implement this as a batched stage in the stream so that it doesn't need to be synchronized
  // batch node additions and removals before constructing the final view to pass to this function
  private def reshardCluster(masters: mutable.Buffer[RedisClusterNode])(implicit saladAPI: SaladAPI): Future[Unit] = synchronized {
    val reshardResults = List.range(0, 16384).toStream.map { slot =>
      saladAPI.clusterSetSlotNode(slot, masters(slot % masters.length).getNodeId)
    }
    val totallyResharded = Future.sequence(reshardResults)
    totallyResharded.onFailure { case e => logger.error(s"Failed to redistribute hash slots", e) }
    totallyResharded.map(_ => logger.info(s"Redistributed hash slots"))
  }

}

