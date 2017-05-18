package com.github.kliewkliew.cornucopia.graph

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.stream.{ClosedShape, FlowShape, ThrottleMode}
import com.github.kliewkliew.cornucopia.redis.Connection.{CodecType, Salad, getConnection, newSaladAPI}
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.github.kliewkliew.cornucopia.redis._
import com.github.kliewkliew.salad.SaladClusterAPI
import com.github.kliewkliew.cornucopia.Config.ReshardTableConfig._
import com.github.kliewkliew.cornucopia.redis.ReshardTable._
import com.lambdaworks.redis.{RedisException, RedisURI}
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.lambdaworks.redis.models.role.RedisInstance.Role

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.language.implicitConversions
import scala.concurrent.{ExecutionContext, Future}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, MergePreferred, Partition, RunnableGraph, Sink}
import com.github.kliewkliew.cornucopia.Config
// TO-DO: put config someplace else

trait CornucopiaGraph {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.github.kliewkliew.cornucopia.CornucopiaException._

  protected val logger = LoggerFactory.getLogger(this.getClass)

  protected def getNewSaladApi: Salad = newSaladAPI

  def partitionEvents(key: String) = key.trim.toLowerCase match {
    case ADD_MASTER.key     => ADD_MASTER.ordinal
    case ADD_SLAVE.key      => ADD_SLAVE.ordinal
    case REMOVE_NODE.key    => REMOVE_NODE.ordinal
    case RESHARD.key        => RESHARD.ordinal
    case _                  => UNSUPPORTED.ordinal
  }

  def partitionNodeRemoval(key: String) = key.trim.toLowerCase match {
    case REMOVE_MASTER.key  => REMOVE_MASTER.ordinal
    case REMOVE_SLAVE.key   => REMOVE_SLAVE.ordinal
    case UNSUPPORTED.key    => UNSUPPORTED.ordinal
  }

  /**
    * Stream definitions for the graph.
    */
  // Extract a tuple of the key and value from a Kafka record.
  case class KeyValue(key: String, value: String, senderRef: Option[ActorRef] = None, newMasterURI: Option[RedisURI] = None)

  // Allows to create Redis URI from the following forms:
  // host OR host:port
  // e.g., redis://127.0.0.1 OR redis://127.0.0.1:7006
  protected def createRedisUri(uri: String): RedisURI = {
    val parts = uri.split(":")
    if (parts.size == 3) {
      val host = parts(1).foldLeft("")((acc, ch) => if (ch != '/') acc + ch else acc)
      RedisURI.create(host, parts(2).toInt)
    }
    else RedisURI.create(uri)
  }

  // Add a master node to the cluster.
  def streamAddMaster(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(_.value)
    .map(createRedisUri)
    .map(getNewSaladApi.canonicalizeURI)
    .groupedWithin(100, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(addNodesToCluster(_))
    .mapAsync(1)(waitForTopologyRefresh[Seq[RedisURI]])
    .map(_ => KeyValue(RESHARD.key, ""))

  // Add a slave node to the cluster, replicating the master that has the fewest slaves.
  def streamAddSlave(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(_.value)
    .map(createRedisUri)
    .map(getNewSaladApi.canonicalizeURI)
    .groupedWithin(100, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(addNodesToCluster(_))
    .mapAsync(1)(waitForTopologyRefresh[Seq[RedisURI]])
    .mapAsync(1)(findMasters)
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", ""))

  // Emit a key-value pair indicating the node type and URI.
  protected def streamRemoveNode(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(_.value)
    .map(createRedisUri)
    .map(getNewSaladApi.canonicalizeURI)
    .mapAsync(1)(emitNodeType)

  // Remove a slave node from the cluster.
  protected def streamRemoveSlave(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(_.value)
    .groupedWithin(100, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(forgetNodes)
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", ""))

  // Redistribute the hash slots among all nodes in the cluster.
  // Execute slot redistribution at most once per configured interval.
  // Combine multiple requests into one request.
  protected def streamReshard(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(record => Seq(record.value))
    .conflate((seq1, seq2) => seq1 ++ seq2)
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .mapAsync(1)(reshardCluster)
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", ""))

  // Throw for keys indicating unsupported operations.
  protected def unsupportedOperation = Flow[KeyValue]
    .map(record => throw new IllegalArgumentException(s"Unsupported operation ${record.key} for ${record.value}"))

  /**
    * Wait for the new cluster topology view to propagate to all nodes in the cluster. May not be strictly necessary
    * since this microservice immediately attempts to notify all nodes of topology updates.
    *
    * @param passthrough The value that will be passed through to the next map stage.
    * @param executionContext The thread dispatcher context.
    * @tparam T
    * @return The unmodified input value.
    */
 protected def waitForTopologyRefresh[T](passthrough: T)(implicit executionContext: ExecutionContext): Future[T] = Future {
    scala.concurrent.blocking(Thread.sleep(Config.Cornucopia.refreshTimeout))
    passthrough
  }

  /**
    * Wait for the new cluster topology view to propagate to all nodes in the cluster. Same version as above, but this
    * time takes two passthroughs and returns tuple of them as future.
    *
    * @param passthrough1 The first value that will be passed through to the next map stage.
    * @param passthrough2 The second value that will be passed through to the next map stage.
    * @param executionContext The thread dispatcher context.
    * @tparam T
    * @tparam U
    * @return The unmodified input value.
    */
  protected def waitForTopologyRefresh2[T, U](passthrough1: T, passthrough2: U)(implicit executionContext: ExecutionContext): Future[(T, U)] = Future {
    scala.concurrent.blocking(Thread.sleep(Config.Cornucopia.refreshTimeout))
    (passthrough1, passthrough2)
  }

  /**
    * Log the current view of the cluster topology.
    *
    * @param executionContext The thread dispatcher context.
    * @return
    */
  protected def logTopology(implicit executionContext: ExecutionContext): Future[Unit] = {
    implicit val saladAPI = getNewSaladApi
    saladAPI.clusterNodes.map { allNodes =>
      val masterNodes = allNodes.filter(Role.MASTER == _.getRole)
      val slaveNodes = allNodes.filter(Role.SLAVE == _.getRole)
      logger.info(s"Master nodes: $masterNodes")
      logger.info(s"Slave nodes: $slaveNodes")
    }
  }

  /**
    * The entire cluster will meet the new nodes at the given URIs. If the connection to a node fails, then retry
    * until it succeeds.
    *
    * @param redisURIList The list of URI of the new nodes.
    * @param executionContext The thread dispatcher context.
    * @return The list of URI if the nodes were met. TODO: emit only the nodes that were successfully added.
    */
  protected def addNodesToCluster(redisURIList: Seq[RedisURI], retries: Int = 0)(implicit executionContext: ExecutionContext): Future[Seq[RedisURI]] = {
    addNodesToClusterPrime(redisURIList).recoverWith {
      case e: CornucopiaRedisConnectionException =>
        logger.error(s"${e.message}: retrying for number ${retries + 1}", e)
        addNodesToCluster(redisURIList, retries + 1)
    }
  }

  protected def addNodesToClusterPrime(redisURIList: Seq[RedisURI])(implicit executionContext: ExecutionContext): Future[Seq[RedisURI]] = {
    implicit val saladAPI = getNewSaladApi

    def getRedisConnection(nodeId: String): Future[Salad] = {
      getConnection(nodeId).recoverWith {
        case e: RedisException => throw CornucopiaRedisConnectionException(s"Add nodes to cluster failed to get connection to node", e)
      }
    }

    saladAPI.clusterNodes.flatMap { allNodes =>
      val getConnectionsToLiveNodes = allNodes.filter(_.isConnected).map(node => getRedisConnection(node.getNodeId))
      Future.sequence(getConnectionsToLiveNodes).flatMap { connections =>
        // Meet every new node from every old node.
        val metResults = for {
          conn <- connections
          uri <- redisURIList
        } yield {
          conn.clusterMeet(uri)
        }
        Future.sequence(metResults).map(_ => redisURIList)
      }
    }
  }

  /**
    * Set the n new slave nodes to replicate the poorest (fewest slaves) n masters.
    *
    * @param redisURIList The list of ip addresses of the slaves that will be added to the cluster. Hostnames are not acceptable.
    * @param executionContext The thread dispatcher context.
    * @return Indicate that the n new slaves are replicating the poorest n masters.
    */
  protected def findMasters(redisURIList: Seq[RedisURI])(implicit executionContext: ExecutionContext): Future[Unit] = {
    implicit val saladAPI = getNewSaladApi
    saladAPI.clusterNodes.flatMap { allNodes =>
      // Node ids for nodes that are currently master nodes but will become slave nodes.
      val newSlaveIds = allNodes.filter(node => redisURIList.contains(node.getUri)).map(_.getNodeId)
      // The master nodes (the nodes that will become slaves are still master nodes at this point and must be filtered out).
      val masterNodes = saladAPI.masterNodes(allNodes)
        .filterNot(node => newSlaveIds.contains(node.getNodeId))
      // HashMap of master node ids to the number of slaves for that master.
      val masterSlaveCount = new util.HashMap[String, AtomicInteger](masterNodes.length + 1, 1)
      // Populate the hash map.
      masterNodes.map(_.getNodeId).foreach(nodeId => masterSlaveCount.put(nodeId, new AtomicInteger(0)))
      allNodes.map { node =>
        Option.apply(node.getSlaveOf)
          .map(master => masterSlaveCount.get(master).incrementAndGet())
      }

      // Find the poorest n masters for n slaves.
      val poorestMasters = new MaxNHeapMasterSlaveCount(redisURIList.length)
      masterSlaveCount.asScala.foreach(poorestMasters.offer)
      assert(redisURIList.length >= poorestMasters.underlying.length)

      // Create a list so that we can circle back to the first element if the new slaves outnumber the existing masters.
      val poorMasterList = poorestMasters.underlying.toList
      val poorMasterIndex = new AtomicInteger(0)
      // Choose a master for every slave.
      val listFuturesResults = redisURIList.map { slaveURI =>
        getConnection(slaveURI).map(_.clusterReplicate(
          poorMasterList(poorMasterIndex.getAndIncrement() % poorMasterList.length)._1))
      }
      Future.sequence(listFuturesResults).map(x => x)
    }
  }

  /**
    * Emit a key-value representing the node-type and the node-id.
    * @param redisURI
    * @param executionContext
    * @return the node type and id.
    */
  def emitNodeType(redisURI:RedisURI)(implicit executionContext: ExecutionContext): Future[KeyValue] = {
    implicit val saladAPI = getNewSaladApi
    saladAPI.clusterNodes.map { allNodes =>
      val removalNodeOpt = allNodes.find(node => node.getUri.equals(redisURI))
      if (removalNodeOpt.isEmpty) throw new Exception(s"Node not in cluster: $redisURI")
      val kv = removalNodeOpt.map { node =>
        node.getRole match {
          case Role.MASTER => KeyValue(RESHARD.key, node.getNodeId)
          case Role.SLAVE  => KeyValue(REMOVE_SLAVE.key, node.getNodeId)
          case _           => KeyValue(UNSUPPORTED.key, node.getNodeId)
        }
      }
      kv.get
    }
  }

  /**
    * Safely remove a master by redistributing its hash slots before blacklisting it from the cluster.
    * The data is given time to migrate as configured in `cornucopia.grace.period`.
    *
    * @param withoutNodes The list of ids of the master nodes that will be removed from the cluster.
    * @param executionContext The thread dispatcher context.
    * @return Indicate that the hash slots were redistributed and the master removed from the cluster.
    */
  protected def removeMasters(withoutNodes: Seq[String])(implicit executionContext: ExecutionContext): Future[Unit] = {
    val reshardDone = reshardCluster(withoutNodes).map { _ =>
      scala.concurrent.blocking(Thread.sleep(Config.Cornucopia.gracePeriod)) // Allow data to migrate.
    }
    reshardDone.flatMap(_ => forgetNodes(withoutNodes))
  }

  /**
    * Notify all nodes in the cluster to forget this node.
    *
    * @param withoutNodes The list of ids of nodes to be forgotten by the cluster.
    * @param executionContext The thread dispatcher context.
    * @return A future indicating that the node was forgotten by all nodes in the cluster.
    */
  def forgetNodes(withoutNodes: Seq[String])(implicit executionContext: ExecutionContext): Future[Unit] =
    if (!withoutNodes.exists(_.nonEmpty))
      Future(Unit)
    else {
      implicit val saladAPI = getNewSaladApi
      saladAPI.clusterNodes.flatMap { allNodes =>
        logger.info(s"Forgetting nodes: $withoutNodes")
        // Reset the nodes to be removed.
        val validWithoutNodes = withoutNodes.filter(_.nonEmpty)
        validWithoutNodes.map(getConnection).map(_.map(_.clusterReset(true)))

        // The nodes that will remain in the cluster should forget the nodes that will be removed.
        val withNodes = allNodes
          .filterNot(node => validWithoutNodes.contains(node.getNodeId)) // Node cannot forget itself.

        // For the cross product of `withNodes` and `withoutNodes`; to remove the nodes in `withoutNodes`.
        val forgetResults = for {
          operatorNode <- withNodes
          operandNodeId <- validWithoutNodes
        } yield {
          if (operatorNode.getSlaveOf == operandNodeId)
            Future(Unit) // Node cannot forget its master.
          else
            getConnection(operatorNode.getNodeId).flatMap(_.clusterForget(operandNodeId))
        }
        Future.sequence(forgetResults).map(x => x)
      }
    }


  /**
    * Reshard the cluster using a view of thecluster consisting of a subset of master nodes.
    *
    * @param withoutNodes The list of ids of nodes that will not be assigned hash slots. Note that this is not used
    *                     (it is empty) when we add a new master node and reshard.
    * @return Boolean indicating that all hash slots were reassigned successfully.
    */
  protected def reshardCluster(withoutNodes: Seq[String])
  : Future[Unit] = {
    // Execute futures using a thread pool so we don't run out of memory due to futures.
    implicit val executionContext = Config.Consumer.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")
    implicit val saladAPI = getNewSaladApi
    saladAPI.masterNodes.flatMap { masterNodes =>

      val liveMasters = masterNodes.filter(_.isConnected)
      lazy val idToURI = new util.HashMap[String,RedisURI](liveMasters.length + 1, 1)
      // Re-use cluster connections so we don't exceed file-handle limit or waste resources.
      lazy val clusterConnections = new util.HashMap[String,Future[SaladClusterAPI[CodecType,CodecType]]](liveMasters.length + 1, 1)
      liveMasters.map { master =>
        idToURI.put(master.getNodeId, master.getUri)
        clusterConnections.put(master.getNodeId, getConnection(master.getNodeId))
      }

      // Remove dead nodes. This may generate WARN logs if some nodes already forgot the dead node.
      val deadMastersIds = masterNodes.filterNot(_.isConnected).map(_.getNodeId)
      logger.info(s"Dead nodes: $deadMastersIds")
      forgetNodes(deadMastersIds)

      // Migrate the data.
      val assignableMasters = liveMasters.filterNot(masterNode => withoutNodes.contains(masterNode.getNodeId))
      logger.info(s"Resharding cluster with ${assignableMasters.map(_.getNodeId)} without ${withoutNodes ++ deadMastersIds}")
      val migrateResults = liveMasters.flatMap { node =>
        val (sourceNodeId, slotList) = (node.getNodeId, node.getSlots.toList.map(_.toInt))
        logger.debug(s"Migrating data from $sourceNodeId among slots $slotList")
        slotList.map { slot =>
          val destinationNodeId = slotNode(slot, assignableMasters)
          migrateSlot(
            slot,
            sourceNodeId, destinationNodeId, idToURI.get(destinationNodeId),
            assignableMasters.toList, clusterConnections)
        }
      }
      val finalMigrateResult = Future.sequence(migrateResults)
      finalMigrateResult.onFailure { case e => logger.error(s"Failed to migrate hash slot data", e) }
      finalMigrateResult.onSuccess { case _ => logger.info(s"Migrated hash slot data") }
      // We attempted to migrate the data but do not prevent slot reassignment if migration fails.
      // We may lose prior data but we ensure that all slots are assigned.
      finalMigrateResult.onComplete { case _ =>
        // This is broken anyways
        //List.range(0, 16384).map(notifySlotAssignment(_, assignableMasters))
        Unit
      }
      finalMigrateResult.flatMap(_ => forgetNodes(withoutNodes))
    }
  }

  // TODO: Put this method out of its misery
  // TODO: pass slotNode as a lambda to migrateSlot and notifySlotAssignment.
  // TODO: more efficient slot assignment to prevent data migration.
  /**
    * Choose a master node for a slot.
    *
    * @param slot The slot to be assigned.
    * @param masters The list of masters that can be assigned slots.
    * @return The node id of the chosen master.
    */
  protected def slotNode(slot: Int, masters: mutable.Buffer[RedisClusterNode]): String =
    masters(slot % masters.length).getNodeId

  /**
    * Migrate all keys in a slot from the source node to the destination node and update the slot assignment on the
    * affected nodes.
    *
    * @param slot The slot to migrate.
    * @param sourceNodeId The current location of the slot data.
    * @param destinationNodeId The target location of the slot data.
    * @param masters The list of nodes in the cluster that will be assigned hash slots.
    * @param clusterConnections The list of connections to nodes in the cluster.
    * @param executionContext The thread dispatcher context.
    * @return Future indicating success.
    */
  protected def migrateSlot(slot: Int, sourceNodeId: String, destinationNodeId: String, destinationURI: RedisURI,
                            masters: List[RedisClusterNode],
                            clusterConnections: util.HashMap[String,Future[SaladClusterAPI[CodecType,CodecType]]],
                            attempts: Int = 1)
                           (implicit saladAPI: Salad, executionContext: ExecutionContext): Future[Unit] = {

    logger.debug(s"Migrate slot for slot $slot from source node $sourceNodeId to target node $destinationNodeId")

    // Follows redis-trib.rb
    def migrateSlotKeys(sourceConn: SaladClusterAPI[CodecType, CodecType],
                        destinationConn: SaladClusterAPI[CodecType, CodecType]): Future[Unit] = {

      import com.github.kliewkliew.salad.serde.ByteArraySerdes._

      // get all the keys in the given slot
      val keyList = for {
        keyCount <- sourceConn.clusterCountKeysInSlot(slot)
        keyList <- sourceConn.clusterGetKeysInSlot[CodecType](slot, keyCount.toInt)
      } yield keyList

      // migrate over all the keys in the slot from source to destination node
      val migrate = for {
        keys <- keyList
        result <- sourceConn.migrate[CodecType](destinationURI, keys.toList)
      } yield result

      migrate.onSuccess { case _ =>
        logger.info(s"Successfully migrated slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} on attempt $attempts")
      }

      def handleFailedMigration(error: Throwable): Future[Unit] = {
        val errorString = error.toString

        def findError(e: String, identifier: String): Boolean = {
          identifier.r.findFirstIn(e) match {
            case Some(_) => true
            case _ => false
          }
        }

        if (findError(errorString, "BUSYKEY")) {
          logger.warn(s"Problem migrating slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} (BUSYKEY): Target key exists. Replacing it for FIX.")
          def migrateReplace: Future[Unit] = for {
            keys <- keyList
            result <- sourceConn.migrate[CodecType](destinationURI, keys.toList, replace = true)
          } yield result
          migrateReplace
        } else if (findError(errorString, "CLUSTERDOWN")) {
          logger.error(s"Failed to migrate slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} (CLUSTERDOWN): Retrying for attempt $attempts")
          migrateSlot(slot, sourceNodeId, destinationNodeId, destinationURI, masters, clusterConnections, attempts + 1)
        } else if (findError(errorString, "MOVED")) {
          logger.error(s"Failed to migrate slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} (MOVED): Ignoring on attempt $attempts")
          Future(Unit)
        } else {
          logger.error(s"Failed to migrate slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost}", error)
          Future(Unit)
        }
      }

      migrate.recover { case e => handleFailedMigration(e) }
    }

    def setSlotAssignment(sourceConn: SaladClusterAPI[CodecType, CodecType],
                        destinationConn: SaladClusterAPI[CodecType, CodecType]): Future[Unit] = {
      for {
        _ <- destinationConn.clusterSetSlotImporting(slot, sourceNodeId)
        _ <- sourceConn.clusterSetSlotMigrating(slot, destinationNodeId)
      } yield {
        Future(Unit)
      }
    }

    destinationNodeId match {
      case `sourceNodeId` =>
        // Don't migrate if the source and destination are the same.
        Future(Unit)
      case _ =>
        for {
          sourceConnection <- clusterConnections.get(sourceNodeId)
          destinationConnection <- clusterConnections.get(destinationNodeId)
          _ <- setSlotAssignment(sourceConnection, destinationConnection)
          _ <- migrateSlotKeys(sourceConnection, destinationConnection)
        } yield {
          logger.debug(s"Migrate slot successful for slot $slot from source node $sourceNodeId to target node $destinationNodeId, notifying masters of new slot assignment")
          notifySlotAssignment(slot, destinationNodeId, masters)
        }
    }
  }

  /**
    * Notify all master nodes of a slot assignment so that they will immediately be able to redirect clients.
    *
    * @param masters The list of nodes in the cluster that will be assigned hash slots.
    * @param assignedNodeId The node that should be assigned the slot
    * @param executionContext The thread dispatcher context.
    * @return Future indicating success.
    */
  protected def notifySlotAssignment(slot: Int, assignedNodeId: String, masters: List[RedisClusterNode])
                                  (implicit saladAPI: Salad, executionContext: ExecutionContext)
  : Future[Unit] = {
    val getMasterConnections = masters.map(master => getConnection(master.getNodeId))
    Future.sequence(getMasterConnections).flatMap { masterConnections =>
      val notifyResults = masterConnections.map(_.clusterSetSlotNode(slot, assignedNodeId))
      Future.sequence(notifyResults).map(x => x)
    }
  }

  /**
    * Store the n poorest masters.
    * Implemented on scala.mutable.PriorityQueue.
    *
    * @param n
    */
  sealed case class MaxNHeapMasterSlaveCount(n: Int) {
    private type MSTuple = (String, AtomicInteger)
    private object MSOrdering extends Ordering[MSTuple] {
      def compare(a: MSTuple, b: MSTuple) = a._2.intValue compare b._2.intValue
    }
    implicit private val ordering = MSOrdering
    val underlying = new mutable.PriorityQueue[MSTuple]

    /**
      * O(1) if the entry is not a candidate for the being one of the poorest n masters.
      * O(log(n)) if the entry is a candidate.
      *
      * @param entry The candidate master-slavecount tuple.
      */
    def offer(entry: MSTuple) =
      if (n > underlying.length) {
        underlying.enqueue(entry)
      }
      else if (entry._2.intValue < underlying.head._2.intValue) {
        underlying.dequeue()
        underlying.enqueue(entry)
      }
  }
}

class CornucopiaKafkaSource extends CornucopiaGraph {
  import Config.Consumer.materializer

  private type KafkaRecord = ConsumerRecord[String, String]

  private def extractKeyValue = Flow[KafkaRecord]
    .map[KeyValue](record => KeyValue(record.key, record.value))

  def run = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import scala.concurrent.ExecutionContext.Implicits.global
    import GraphDSL.Implicits._

    val in = Config.Consumer.cornucopiaKafkaSource
    val out = Sink.ignore

    val mergeFeedback = builder.add(MergePreferred[KeyValue](2))

    val partition = builder.add(Partition[KeyValue](
      5, kv => partitionEvents(kv.key)))

    val kv = builder.add(extractKeyValue)

    val partitionRm = builder.add(Partition[KeyValue](
      3, kv => partitionNodeRemoval(kv.key)
    ))

    in                                      ~> kv
    kv                                      ~> mergeFeedback.preferred
    mergeFeedback.out                       ~> partition
    partition.out(ADD_MASTER.ordinal)       ~> streamAddMaster      ~> mergeFeedback.in(0)
    partition.out(ADD_SLAVE.ordinal)        ~> streamAddSlave       ~> out
    partition.out(REMOVE_NODE.ordinal)      ~> streamRemoveNode     ~> partitionRm
    partitionRm.out(REMOVE_MASTER.ordinal)  ~> mergeFeedback.in(1)
    partitionRm.out(REMOVE_SLAVE.ordinal)   ~> streamRemoveSlave    ~> out
    partitionRm.out(UNSUPPORTED.ordinal)    ~> unsupportedOperation ~> out
    partition.out(RESHARD.ordinal)          ~> streamReshard        ~> out
    partition.out(UNSUPPORTED.ordinal)      ~> unsupportedOperation ~> out

    ClosedShape
  }).run()

}

class CornucopiaActorSource extends CornucopiaGraph {
  import Config.Consumer.materializer
  import com.github.kliewkliew.cornucopia.actors.CornucopiaSource.Task
  import scala.concurrent.ExecutionContext.Implicits.global

  protected type ActorRecord = Task

  // Add a master node to the cluster.
  override def streamAddMaster(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(kv => (kv.value, kv.senderRef))
    .map(t => (createRedisUri(t._1), t._2) )
    .map(t => (getNewSaladApi.canonicalizeURI(t._1), t._2))
    .groupedWithin(1, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(t => {
      val t1 = t.unzip
      val redisURIs = t1._1
      val actorRefs = t1._2
      addNodesToCluster(redisURIs) flatMap { uris =>
        waitForTopologyRefresh2[Seq[RedisURI], Seq[Option[ActorRef]]](uris, actorRefs)
      }
    })
    .map{ case (redisURIs, actorRef) =>
      val ref = actorRef.head
      val uri = redisURIs.head
      KeyValue(RESHARD.key, "", ref, Some(uri))
    }

  // Add a slave node to the cluster, replicating the master that has the fewest slaves.
  protected def streamAddSlavePrime(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(kv => (kv.value, kv.senderRef))
    .map(t => (createRedisUri(t._1), t._2) )
    .map(t => (getNewSaladApi.canonicalizeURI(t._1), t._2))
    .groupedWithin(1, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(t => {
      val t1 = t.unzip
      val redisURIs = t1._1
      val actorRefs = t1._2
      addNodesToCluster(redisURIs) flatMap { uris =>
        waitForTopologyRefresh2[Seq[RedisURI], Seq[Option[ActorRef]]](uris, actorRefs)
      }
    })
    .mapAsync(1)(t => {
      val redisURIs = t._1
      val actorRefs = t._2
      findMasters(redisURIs) map { _ =>
        actorRefs
      }
    })
    .mapAsync(1)(waitForTopologyRefresh[Seq[Option[ActorRef]]])
    .mapAsync(1)(signalSlavesAdded)
    .map(_ => KeyValue("", ""))

  private def signalSlavesAdded(senders: Seq[Option[ActorRef]]): Future[Unit] = {
    def signal(ref: ActorRef): Future[Unit] = {
      Future {
        ref ! Right("slave")
      }
    }
    val flattened = senders.flatten
    if (flattened.isEmpty) Future(Unit)
    else Future.reduce(senders.flatten.map(signal))((_, _) => Unit)
  }

  override protected def streamReshard(implicit executionContext: ExecutionContext) = Flow[KeyValue]
    .map(kv => (kv.senderRef, kv.newMasterURI))
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .mapAsync(1)(t => {
      val senderRef = t._1
      val newMasterURI = t._2
      reshardClusterPrime(senderRef, newMasterURI)
    })
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", ""))

  protected def reshardClusterPrime(sender: Option[ActorRef], newMasterURI: Option[RedisURI], retries: Int = 0): Future[Unit] = {

    def reshard(ref: ActorRef, uri: RedisURI): Future[Unit] = {
      reshardClusterWithNewMaster(uri) map { _: Unit =>
        logger.info(s"Successfully resharded cluster ($retries retries), informing Kubernetes controller")
        ref ! Right("master")
      } recover {
        case e: ReshardTableException =>
          logger.error(s"There was a problem computing the reshard table, retrying for retry number ${retries + 1}:", e)
          reshardClusterPrime(sender, newMasterURI, retries + 1)
        case ex: Throwable =>
          logger.error("Failed to reshard cluster, informing Kubernetes controller", ex)
          ref ! Left(s"${ex.toString}")
      }
    }

    val result = for {
      ref <- sender
      uri <- newMasterURI
    } yield reshard(ref, uri)

    result match {
      case Some(f) => f
      case None =>
        // this should never happen though
        logger.error("There was a problem resharding the cluster: sender actor or new redis master URI missing")
        Future(Unit)
    }
  }

  private def printReshardTable(reshardTable: Map[String, List[Int]]) = {
    logger.info(s"Reshard Table:")
    reshardTable foreach { case (nodeId, slots) =>
        logger.info(s"Migrating slots from node '$nodeId': ${slots.mkString(", ")}")
    }
  }

  protected def reshardClusterWithNewMaster(newMasterURI: RedisURI)
  : Future[Unit] = {
    // Execute futures using a thread pool so we don't run out of memory due to futures.
    implicit val executionContext = Config.Consumer.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")

    implicit val saladAPI = getNewSaladApi

    saladAPI.masterNodes.flatMap { mn =>
      val masterNodes = mn.toList

      logger.debug(s"Reshard table with new master master nodes: ${masterNodes.map(_.getNodeId)}")

      val liveMasters = masterNodes.filter(_.isConnected)

      logger.debug(s"Reshard cluster with new master live masters: ${liveMasters.map(_.getNodeId)}")

      lazy val idToURI = new util.HashMap[String,RedisURI](liveMasters.length + 1, 1)

      // Re-use cluster connections so we don't exceed file-handle limit or waste resources.
      lazy val clusterConnections = new util.HashMap[String,Future[SaladClusterAPI[CodecType,CodecType]]](liveMasters.length + 1, 1)

      val targetNode = masterNodes.filter(_.getUri == newMasterURI).head

      logger.debug(s"Reshard cluster with new master target node: ${targetNode.getNodeId}")

      liveMasters.map { master =>
        idToURI.put(master.getNodeId, master.getUri)
        val connection = getConnection(master.getNodeId)
        clusterConnections.put(master.getNodeId, connection)
      }

      logger.debug(s"Reshard cluster with new master cluster connections for nodes: ${clusterConnections.keySet().toString}")

      val sourceNodes = masterNodes.filterNot(_ == targetNode)

      logger.debug(s"Reshard cluster with new master source nodes: ${sourceNodes.map(_.getNodeId)}")

      val reshardTable = computeReshardTable(sourceNodes)

      printReshardTable(reshardTable)

      val migrateResults = for {
        (sourceNodeId, slots) <- reshardTable
        slot <- slots
      } yield {
        migrateSlot(slot, sourceNodeId, targetNode.getNodeId, newMasterURI, liveMasters, clusterConnections)
      }

      Future.fold(migrateResults)()((_, b) => b)
    }
  }

  protected def extractKeyValue = Flow[ActorRecord]
    .map[KeyValue](record => KeyValue(record.operation, record.redisNodeIp, record.ref))

  protected val processTask = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val taskSource = builder.add(Flow[Task])

    val mergeFeedback = builder.add(MergePreferred[KeyValue](2))

    val partition = builder.add(Partition[KeyValue](
      5, kv => partitionEvents(kv.key)))

    val kv = builder.add(extractKeyValue)

    val partitionRm = builder.add(Partition[KeyValue](
      3, kv => partitionNodeRemoval(kv.key)
    ))

    val fanIn = builder.add(Merge[KeyValue](5))

    taskSource.out                          ~> kv
    kv                                      ~> mergeFeedback.preferred
    mergeFeedback.out                       ~> partition
    partition.out(ADD_MASTER.ordinal)       ~> streamAddMaster      ~> mergeFeedback.in(0)
    partition.out(ADD_SLAVE.ordinal)        ~> streamAddSlavePrime  ~> fanIn
    partition.out(REMOVE_NODE.ordinal)      ~> streamRemoveNode     ~> partitionRm
    partitionRm.out(REMOVE_MASTER.ordinal)  ~> mergeFeedback.in(1)
    partitionRm.out(REMOVE_SLAVE.ordinal)   ~> streamRemoveSlave    ~> fanIn
    partitionRm.out(UNSUPPORTED.ordinal)    ~> unsupportedOperation ~> fanIn
    partition.out(RESHARD.ordinal)          ~> streamReshard        ~> fanIn
    partition.out(UNSUPPORTED.ordinal)      ~> unsupportedOperation ~> fanIn

    FlowShape(taskSource.in, fanIn.out)
  })

  protected val cornucopiaSource = Config.Consumer.cornucopiaActorSource

  def ref: ActorRef = processTask
    .to(Sink.ignore)
    .runWith(cornucopiaSource)

}
