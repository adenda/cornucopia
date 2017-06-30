package com.adendamedia.cornucopia.graph

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor._
import akka.stream.{FlowShape, ThrottleMode}
import com.adendamedia.cornucopia.redis.Connection.{CodecType, Salad, getConnection, newSaladAPI}
import org.slf4j.LoggerFactory
import com.adendamedia.cornucopia.redis._
import com.adendamedia.salad.SaladClusterAPI
import com.adendamedia.cornucopia.Config.ReshardTableConfig._
import com.adendamedia.cornucopia.redis.ReshardTable._
import com.lambdaworks.redis.{RedisException, RedisURI}
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.lambdaworks.redis.models.role.RedisInstance.Role

import collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.concurrent.{ExecutionContext, Future, blocking}
import akka.stream.scaladsl.{Flow, GraphDSL, Merge, MergePreferred, Partition, Sink}
import com.adendamedia.cornucopia.Config
import com.adendamedia.cornucopia.actors.{RedisCommandRouter, SharedActorSystem}

import scala.annotation.tailrec

trait CornucopiaGraph {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.adendamedia.cornucopia.CornucopiaException._

  type ClusterConnectionHashMap = util.HashMap[String,Future[SaladClusterAPI[CodecType,CodecType]]]

  protected val logger = LoggerFactory.getLogger(this.getClass)

  protected def getNewSaladApi: Salad = newSaladAPI
  protected def getNewSaladApiForUri: RedisURI => Salad = newSaladAPI(_: RedisURI)

  def partitionEvents(key: String) = key.trim.toLowerCase match {
    case ADD_MASTER.key          => ADD_MASTER.ordinal
    case ADD_SLAVE.key           => ADD_SLAVE.ordinal
    case REMOVE_MASTER.key       => REMOVE_MASTER.ordinal
    case REMOVE_SLAVE.key        => REMOVE_SLAVE.ordinal
    case RESHARD.key             => RESHARD.ordinal
    case CLUSTER_TOPOLOGY.key    => CLUSTER_TOPOLOGY.ordinal
    case _                       => UNSUPPORTED.ordinal
  }

  // TODO: remove this since we want to control when a master or slave is removed, and give the URI of the node that
  // is or should become that node to be removed.
  def partitionNodeRemoval(key: String) = key.trim.toLowerCase match {
    case REMOVE_MASTER.key  => REMOVE_MASTER.ordinal
    case REMOVE_SLAVE.key   => REMOVE_SLAVE.ordinal
    case UNSUPPORTED.key    => UNSUPPORTED.ordinal
  }

  /**
    * Stream definitions for the graph.
    */
  // Extract a tuple of the key and value from a Kafka record.
  case class KeyValue(key: String, value: Option[String], senderRef: Option[ActorRef] = None, newMasterURI: Option[RedisURI] = None)

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

  protected def streamAddMaster(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed]

  // Add a slave node to the cluster, replicating the master that has the fewest slaves.
  protected def streamAddSlave(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed]

  // Emit a KeyValue indicating the node type and URI and sender actor ref
  protected def streamRemoveNode(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] = Flow[KeyValue]
    .map(kv => (kv.value, kv.senderRef))
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .map(t => (createRedisUri(t._1.get), t._2) )
    .map(t => (getNewSaladApi.canonicalizeURI(t._1), t._2))
    .mapAsync(1)(t => {
      val redisURI = t._1
      val actorRef = t._2
      emitNodeType(redisURI) map { case KeyValue(key, value, _, _) =>
        KeyValue(key, value, actorRef)
      }
    })

  // Remove a slave node from the cluster.
  protected def streamRemoveSlave(implicit executionContext: ExecutionContext) = Flow[KeyValue]
//    .map(_.value)
    .map { kv =>
      kv.value match {
        case Some(nodeId) =>
          nodeId
        case None =>
          ""
      }
    }
    .groupedWithin(100, Config.Cornucopia.batchPeriod)
    .mapAsync(1)(forgetNodes)
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", None))

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
          case Role.MASTER => KeyValue(REMOVE_MASTER.key, Some(node.getNodeId))
          case Role.SLAVE  => KeyValue(REMOVE_SLAVE.key, Some(node.getNodeId))
          case _           => KeyValue(UNSUPPORTED.key, Some(node.getNodeId))
        }
      }
      kv.get
    }
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
                            clusterConnections: util.HashMap[String,Future[SaladClusterAPI[CodecType,CodecType]]])
                           (implicit saladAPI: Salad, executionContext: ExecutionContext): Future[Unit] = {

    logger.debug(s"Migrate slot for slot $slot from source node $sourceNodeId to target node $destinationNodeId")

    // Follows redis-trib.rb
    def migrateSlotKeys(sourceConn: SaladClusterAPI[CodecType, CodecType],
                        destinationConn: SaladClusterAPI[CodecType, CodecType], attempts: Int = 1): Future[Unit] = {

      import com.adendamedia.salad.serde.ByteArraySerdes._

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
          for {
            src <- clusterConnections.get(sourceNodeId)
            dst <- clusterConnections.get(destinationNodeId)
            msk <- migrateSlotKeys(src, dst, attempts + 1)
          } yield msk
        } else if (findError(errorString, "MOVED")) {
          logger.error(s"Failed to migrate slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} (MOVED): Ignoring on attempt $attempts")
          Future(Unit)
        } else {
          logger.error(s"Failed to migrate slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost}", error)
          Future(Unit)
        }
      }

      migrate map  { _ =>
        logger.info(s"Successfully migrated slot $slot from $sourceNodeId to $destinationNodeId at ${destinationURI.getHost} on attempt $attempts")
      } recoverWith { case e => handleFailedMigration(e) }

    }

    def setSlotAssignment(sourceConn: SaladClusterAPI[CodecType, CodecType],
                          destinationConn: SaladClusterAPI[CodecType, CodecType],
                          attempts: Int = 1): Future[Unit] = {
      val ssa = for {
        _ <- destinationConn.clusterSetSlotImporting(slot, sourceNodeId)
        _ <- sourceConn.clusterSetSlotMigrating(slot, destinationNodeId)
      } yield { }

      ssa map { _ =>
        logger.info(s"Successfully set slot assignment for slot $slot on attempt $attempts")
      } recover { case e =>
        logger.error(s"There was a problem setting slot assignment for slot $slot, retrying for attempt $attempts: ${e.toString}")
        setSlotAssignment(sourceConn, destinationConn, attempts + 1)
      }

    }

    destinationNodeId match {
      case `sourceNodeId` =>
        // Don't migrate if the source and destination are the same.
        logger.warn(s"Ignoring attempt to migrate slot $slot because source and destination node are the same")
        Future(Unit)
      case _ =>
        for {
          src <- clusterConnections.get(sourceNodeId)
          dst <- clusterConnections.get(destinationNodeId)
          _ <- setSlotAssignment(src, dst)
          _ <- migrateSlotKeys(src, dst)
          _ <- notifySlotAssignment(slot, destinationNodeId, masters)
        } yield {
          logger.info(s"Migrate slot successful for slot $slot from source node $sourceNodeId to target node $destinationNodeId, notifying masters of new slot assignment")
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

class CornucopiaActorSource extends CornucopiaGraph {
  import Config.materializer
  import com.adendamedia.cornucopia.actors.CornucopiaSource.Task
  import scala.concurrent.ExecutionContext.Implicits.global

  protected type ActorRecord = Task

  val actorSystem = SharedActorSystem.sharedActorSystem

  val redisCommandRouter = actorSystem.actorOf(RedisCommandRouter.props, "redisCommandRouter")

  // Add a master node to the cluster.
  override protected def streamAddMaster(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] = Flow[KeyValue]
    .map(kv => (kv.value, kv.senderRef))
    .map(t => (createRedisUri(t._1.get), t._2) )
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
      KeyValue(RESHARD.key, None, ref, Some(uri))
    }

  // Add a slave node to the cluster, replicating the master that has the fewest slaves.
  override protected def streamAddSlave(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] = Flow[KeyValue]
    .map { kv => kv.value match {
      case Some(nodeId) => (kv.value, kv.senderRef)
      case None =>
        throw new Exception(s"Node Id is required to add a new slave") // TODO: handle exceptions so as not to crash app?
      }
    }
    .map(t => (createRedisUri(t._1.get), t._2) )
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
        (redisURIs, actorRefs)
      }
    })
    .mapAsync(1)(t => {
      val redisURIs = t._1
      val actorRefs = t._2
      waitForTopologyRefresh2[Seq[RedisURI], Seq[Option[ActorRef]]](redisURIs, actorRefs)
    })
    .mapAsync(1)(t => signalSlavesAdded(t._1, t._2))
    .map(_ => KeyValue("", None))

  private def signalSlavesAdded(uris: Seq[RedisURI], senders: Seq[Option[ActorRef]]): Future[Unit] = {
    def signal(uri: RedisURI, ref: Option[ActorRef]): Future[Unit] = {
      Future {
        ref match {
          case Some(sender) => sender ! Right(("slave", uri.getHost))
          case None => Unit
        }
      }
    }
    val flattened = senders.flatten
    if (flattened.isEmpty) Future(Unit)
    else {
      val zipped = uris zip senders

      Future.reduce(
        zipped map {
          case (uri: RedisURI, sender: Option[ActorRef]) => signal(uri, sender)
        }
      )((_, _) => Unit)
    }
  }

  protected def streamReshard(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] = Flow[KeyValue]
    .map(kv => (kv.senderRef, kv.newMasterURI))
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .mapAsync(1)(t => {
      val senderRef = t._1
      val newMasterURI = t._2
      reshardClusterPrime(senderRef, newMasterURI)
    })
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", None))

  // Reshard the cluster by growing with the new master that was just added
  protected def reshardClusterPrime(sender: Option[ActorRef], newMasterURI: Option[RedisURI], retries: Int = 0): Future[Unit] = {

    def reshard(ref: ActorRef, uri: RedisURI): Future[Unit] = {
      reshardClusterWithNewMaster(uri) map { _: Unit =>
        logger.info(s"Successfully resharded cluster ($retries retries), informing Kubernetes controller")
        ref ! Right(("master", uri.getHost))
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

  private def printReshardTablePrime(reshardTable: Map[String, List[Int]]) = {
    logger.info(s"Reshard Table:")
    reshardTable foreach { case (nodeId, slots) =>
      logger.info(s"Migrating slots to target node '$nodeId': ${slots.mkString(", ")}")
    }
  }

  protected def reshardClusterWithNewMaster(newMasterURI: RedisURI)
  : Future[Unit] = {

    // Execute futures using a thread pool so we don't run out of memory due to futures.
    implicit val executionContext = Config.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")

    implicit val saladAPI = getNewSaladApi

    saladAPI.masterNodes.flatMap { mn =>
      val masterNodes = mn.toList

      logger.debug(s"Reshard table with new master nodes: ${masterNodes.map(_.getNodeId)}")

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

      def doReshard: Future[Unit] = {
        // Since migrating many slots causes many requests to redis cluster nodes, we should have a way to throttle
        // the number of parallel futures executing at any given time so that we don't flood redis nodes with too many
        // simultaneous requests.
        import akka.pattern.ask
        import akka.util.Timeout
        import scala.concurrent.duration._
        import RedisCommandRouter._

        implicit val timeout = Timeout(Config.reshardTimeout seconds)

        val migrateSlotFn = migrateSlot(_: Int, _: String, _: String, newMasterURI, liveMasters, clusterConnections)

        val future = redisCommandRouter ? ReshardCluster(targetNode.getNodeId, reshardTable, migrateSlotFn)

        future.mapTo[String].map { msg: String =>
          logger.info(s"Reshard cluster was a success: $msg")
          Unit
        }
      }

      def waitForNewNodeToBeOk(conn: SaladClusterAPI[Connection.CodecType, Connection.CodecType]): Future[Unit] = {
        def isOk(info: Map[String,String]): Boolean = info("cluster_state") == "ok"
        conn.clusterInfo flatMap { info: Map[String,String] =>
          if (isOk(info)) {
            logger.info(s"New node is ready for resharding")
            doReshard
          }
          else {
            logger.warn(s"New node is not yet ready for resharding, keep waiting")
            Thread.sleep(100)
            waitForNewNodeToBeOk(conn)
          }
        }
      }

      clusterConnections.get(targetNode.getNodeId) flatMap waitForNewNodeToBeOk
    }
  }

  /**
    * Remove a master node, but not before redistributing its hash slots (i.e., resharding). The KeyValue value given
    * is the URI of the actual node that should be removed. If that URI is not a master node, then this runs a manual
    * failover on the slave node that occupies this URI. Once the fail over is completed, then the master that now
    * occupies the given URI has all its hash slots distributed to the remaining masters before being forgotten by the
    * cluster.
    *
    * @param executionContext
    * @return
    */
  protected def streamRemoveMaster(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] = Flow[KeyValue]
    .map(kv => (kv.value, kv.senderRef)) // kv.value is URI of the actual redis node that should be removed
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .mapAsync(1)( t => {
      val uri = t._1 // uri of node to remove
      val senderRef = t._2
      reshardClusterSecunde(senderRef, uri)
    })
    .mapAsync(1)(waitForTopologyRefresh[Unit])
    .mapAsync(1)(_ => logTopology)
    .map(_ => KeyValue("", None))

  /**
    * Reshard the cluster by removing a master that must occupy the node with uri of nodeURI. On completion, the sender
    * actor ref is sent a tell message of an Either[String, (String,String)], with Left[String] being an error, and
    * Right[(String,String)] being success containing the node type that was removed, as well as the URI of that node
    * that was forgotten from the cluster.
    *
    * @param sender The actor ref of the caller that is signalled when this operation completes
    * @param nodeURI The uri of the node that should be removed
    * @param retries The number of retries before succeeding
    * @return Future[Unit] on success or failure
    */
  protected def reshardClusterSecunde(sender: Option[ActorRef], nodeURI: Option[String], retries: Int = 0): Future[Unit] = {

    def reshard(ref: ActorRef, uri: String): Future[Unit] = {
      reshardClusterWithoutRetiredMaster(uri) map { _: Unit =>
        logger.info(s"Successfully resharded cluster ($retries retries), informing Kubernetes controller, removing retired master")
        // TODO: remove retired master
        ref ! Right(("master", uri))
      } recover {
        case e: ReshardTableException =>
          logger.error(s"There was a problem computing the reshard table, retrying for retry number ${retries + 1}:", e)
          reshardClusterSecunde(sender, nodeURI, retries + 1)
        case ex: Throwable =>
          logger.error("Failed to reshard cluster, informing Kubernetes controller", ex)
          ref ! Left(s"${ex.toString}")
      }
    }

    val result = for {
      ref <- sender
      uri <- nodeURI
    } yield reshard(ref, uri)

    result match {
      case Some(f) => f
      case None =>
        // this should never happen though
        logger.error("There was a problem resharding the cluster: sender actor or new redis master URI missing")
        Future(Unit)
    }
  }

  protected def reshardClusterWithoutRetiredMaster(retiredMasterNodeUriString: String): Future[Unit] = {
    // Execute futures using a thread pool so we don't run out of memory due to futures.
    implicit val executionContext = Config.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")
    implicit val saladAPI = getNewSaladApi

    val retiredMasterNodeUri = createRedisUri(retiredMasterNodeUriString)

    // By the time this function is called the master node that we are removing should be located on the network node,
    // or instance, with the uri of retiredMasterNodeUriString
    def doReshard(retiredNode: RedisClusterNode): Future[Unit] = {
      logger.info("Doing reshard now")
      Future(Unit)
    }
    def doReshardNope(retiredNode: RedisClusterNode): Future[Unit] = {

      saladAPI.masterNodes map { mn =>

        val masterNodes = mn.toList

        val liveMasters = masterNodes.filter(_.isConnected)
        val remainingMasters = masterNodes.filter(_.getUri != retiredMasterNodeUri)

        logger.debug(s"Reshard cluster without retired master node: ${retiredNode.getNodeId}")
        logger.debug(s"Reshard cluster keeping existing master nodes: ${remainingMasters.map(_.getNodeId)}")

        // Re-use cluster connections so we don't exceed file-handle limit or waste resources.
        lazy val clusterConnections =
          new util.HashMap[String, Future[SaladClusterAPI[CodecType, CodecType]]](liveMasters.length)

        liveMasters.map { master =>
          val connection = getConnection(master.getNodeId)
          clusterConnections.put(master.getNodeId, connection)
        }

        val idToURI: Map[String, RedisURI] = liveMasters.foldLeft(Map.empty[String, RedisURI]) { case (tbl, master) =>
          tbl + (master.getNodeId -> master.getUri)
        }

        logger.debug(s"Reshard cluster with new master cluster connections for nodes: ${clusterConnections.keySet().toString}")

        val reshardTable = computeReshardTablePrime(retiredNode, remainingMasters)

        printReshardTablePrime(reshardTable)

        // Since migrating many slots causes many requests to redis cluster nodes, we should have a way to throttle
        // the number of parallel futures executing at any given time so that we don't flood redis nodes with too many
        // simultaneous requests.
        import akka.pattern.ask
        import akka.util.Timeout
        import scala.concurrent.duration._
        import RedisCommandRouter._

        implicit val timeout = Timeout(Config.reshardTimeout seconds)

        val migrateSlotFn = migrateSlot(_: Int, _: String, _: String, _: RedisURI, liveMasters, clusterConnections)

        val retiredMasterNodeId: String = retiredNode.getNodeId

        val future = redisCommandRouter ? ReshardClusterPrime(retiredMasterNodeId, reshardTable, migrateSlotFn, idToURI)

        future.mapTo[String].map { msg: String =>
          logger.info(s"Reshard cluster was a success: $msg")
          logger.info(s"Forgetting retired master with node Id $retiredMasterNodeId")
          forgetNodes(List(retiredMasterNodeId)) map { _ =>
            logger.info(s"Successfully forgot retired master with node Id $retiredMasterNodeId")
          }
        }
      }
    }

    val failoverRetryPeriod = 500 // TODO: put in config

    def waitForFailover(retiredNode: RedisClusterNode): Future[Unit] = {
      saladAPI.masterNodes flatMap { mn =>
        val masters: List[RedisClusterNode] = mn.toList
        // This doesn't short-circuit but it's probably not a big deal since this still runs in O(n)
        val res: Boolean = masters.foldLeft(false)((b: Boolean, master: RedisClusterNode) =>
          b || (master.getUri == retiredNode.getUri)
        )
        if (res) Future(Unit)
        else {
          logger.info(s"Failover not yet complete, will check again in $failoverRetryPeriod ms.")
          blocking(Thread.sleep(failoverRetryPeriod))
          waitForFailover(retiredNode)
        }
      }
    }

    def failover(retiredNode: RedisClusterNode): Future[Unit] = {
      // redisNode is currently a slave node that must be failed over to become a master
      val saladApiForUri: Salad = getNewSaladApiForUri(retiredNode.getUri)

      saladApiForUri.clusterFailover().flatMap { _ =>
        waitForFailover(retiredNode) map { _ =>
          logger.info(s"Failover complete: network node with URI ${retiredNode.getUri.toURI} is now a Redis master.")
          doReshard(retiredNode)
        }
      }
    }

    val res = for {
      mn <- saladAPI.masterNodes
      sn <- saladAPI.slaveNodes
    } yield {
      val masterNodes = mn.toList
      val slaveNodes = sn.toList

      // The network node to be retired could be either a master node or a slave node
      val retiredNode = masterNodes.filter(_.getUri == retiredMasterNodeUri) match {
        case List(n) => n
        case List() => slaveNodes.filter(_.getUri == retiredMasterNodeUri).head
      }

      // If the retired node is already a master, then we can go ahead and reshard. If not, then we need to fail-over
      // on the slave Redis cluster node that occupies this instance with the given URI, before resharding.
      if (retiredNode.getFlags contains "MASTER") {
        doReshard(retiredNode)
      }
      else {
        logger.info(s"Network node with URI ${retiredNode.getUri.toURI} to be removed is not a master. Running a manual failover so it becomes a master node before resharding.")
        failover(retiredNode)
      }

    }
    val b = res.flatMap(x => x)
    b
  }

  protected def streamGetClusterTopology(implicit executionContext: ExecutionContext): Flow[KeyValue, KeyValue, NotUsed] =
    Flow[KeyValue]
    .map(kv => kv.senderRef)
    .throttle(1, Config.Cornucopia.minReshardWait, 1, ThrottleMode.Shaping)
    .mapAsync(1) { case Some(ref) =>
      ClusterTopology.getClusterTopology map { topology =>
        logger.info(s"Got cluster topology: $topology")
        ref ! topology
        ref
      }
    }
    .map(_ => KeyValue("", None, Some(ref)))

  protected def extractKeyValue = Flow[ActorRecord]
    .map[KeyValue](record => KeyValue(record.operation, record.redisNodeIp, record.ref))

  protected val processTask = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val taskSource = builder.add(Flow[Task])

    val mergeFeedback = builder.add(MergePreferred[KeyValue](1))

    val partition = builder.add(Partition[KeyValue](
      7, kv => partitionEvents(kv.key)))

    val kv = builder.add(extractKeyValue)

    val fanIn = builder.add(Merge[KeyValue](6))

    taskSource.out                             ~> kv
    kv                                         ~> mergeFeedback.preferred
    mergeFeedback.out                          ~> partition
    partition.out(ADD_MASTER.ordinal)          ~> streamAddMaster           ~> mergeFeedback.in(0)
    partition.out(ADD_SLAVE.ordinal)           ~> streamAddSlave            ~> fanIn
    partition.out(REMOVE_MASTER.ordinal)       ~> streamRemoveMaster        ~> fanIn
    partition.out(REMOVE_SLAVE.ordinal)        ~> streamRemoveSlave         ~> fanIn
    partition.out(RESHARD.ordinal)             ~> streamReshard             ~> fanIn
    partition.out(CLUSTER_TOPOLOGY.ordinal)    ~> streamGetClusterTopology  ~> fanIn
    partition.out(UNSUPPORTED.ordinal)         ~> unsupportedOperation      ~> fanIn

    FlowShape(taskSource.in, fanIn.out)
  })

  protected val cornucopiaSource = Config.cornucopiaActorSource

  def ref: ActorRef = processTask
    .to(Sink.ignore)
    .runWith(cornucopiaSource)

}
