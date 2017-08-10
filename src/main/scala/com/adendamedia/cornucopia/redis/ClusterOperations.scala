package com.adendamedia.cornucopia.redis

//import com.adendamedia.cornucopia.CornucopiaException._
import org.slf4j.LoggerFactory
import com.adendamedia.cornucopia.redis.Connection._
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.salad.SaladClusterAPI

import scala.concurrent.{ExecutionContext, Future, blocking}
import com.lambdaworks.redis.{RedisCommandExecutionException, RedisException, RedisURI}

object ClusterOperations {

  @SerialVersionUID(1L)
  case class CornucopiaRedisConnectionException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class SetSlotAssignmentException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotKeysBusyKeyException(message: String = "BUSYKEY", reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotKeysClusterDownException(message: String = "CLUSTERDOWN", reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotKeysMovedException(message: String = "MOVED", reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotKeysInputOutputException(message: String = "IOERR", reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class ReplicateMasterException(message: String) extends Throwable(message) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaGetRoleException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaFailoverException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaFailoverVerificationFailedException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaForgetNodeException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaFindPoorestMasterException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  @SerialVersionUID(1L)
  case class CornucopiaGetRedisSourceNodesException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  type NodeId = String
  type RedisUriString = String
  type ClusterConnectionsType = Map[NodeId, Connection.Salad]
  type RedisUriToNodeId = Map[RedisURI, NodeId]
  type NodeIdToRedisUri = Map[NodeId, RedisURI]

  trait Role
  case object Slave extends Role
  case object Master extends Role
}

trait ClusterOperations {
  import ClusterOperations._

  def addNodeToCluster(redisURI: RedisURI)(implicit executionContext: ExecutionContext): Future[RedisURI]

  def getRedisSourceNodes(targetRedisURI: RedisURI)
                         (implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]]

  /**
    * Gets the target nodes and the retired node
    * @param retiredRedisURI The redis URI of the node being retired
    * @param executionContext The execution context
    * @return Future of tuple containing a list of target redis cluster nodes receiving the hash slots from the retired
    *         node, and the retired node
    */
  def getRedisTargetNodesAndRetiredNode(retiredRedisURI: RedisURI)
                                       (implicit executionContext: ExecutionContext):
    Future[(List[RedisClusterNode], RedisClusterNode)]

  def getRedisMasterNodes(implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]]

  def getClusterConnections(implicit executionContext: ExecutionContext): Future[(ClusterConnectionsType, RedisUriToNodeId, SaladAPI)]

  /**
    * Checks if the cluster status of all Redis node connections is "OK"
    * @param clusterConnections The connections to cluster nodes (masters)
    * @param executionContext The Execution context
    * @return Future Boolean, true if all nodes are OK, false otherwise
    */
  def isClusterReady(clusterConnections: ClusterConnectionsType)
                    (implicit executionContext: ExecutionContext): Future[Boolean]

  def setSlotAssignment(slot: Slot, sourceNodeId: NodeId, targetNodeId: NodeId,
                        clusterConnections: ClusterConnectionsType)
                       (implicit executionContext: ExecutionContext): Future[Unit]

  def migrateSlotKeys(slot: Slot, targetRedisURI: RedisURI, sourceNodeId: NodeId, targetNodeId: NodeId,
                      clusterConnections: ClusterConnectionsType)
                     (implicit executionContext: ExecutionContext): Future[Unit]

  def notifySlotAssignment(slot: Slot, assignedNodeId: NodeId, clusterConnections: ClusterConnectionsType)
                          (implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Find the master with the fewest slaves
    * @param clusterConnections Cluster connections of master nodes
    * @param executionContext The execution context
    * @return Future containing the redis URI of the poorest master
    */
  def findPoorestMaster(clusterConnections: ClusterConnectionsType)
                       (implicit executionContext: ExecutionContext): Future[NodeId]

  /**
    * Find the master with the fewest slaves
    * @param executionContext The execution context
    * @return Future containing the redis URI of the poorest master
    */
  def findPoorestMaster(implicit executionContext: ExecutionContext): Future[NodeId]

  /**
    * Find the poorest master excluding the masters in the provide excluded List
    * @param clusterConnections Cluster connections of master nodes
    * @param excludedMasters Master to exclude
    * @param executionContext The execution context
    * @return Future containing the redis URI of the poorest remaining master
    */
  def findPoorestRemainingMaster(clusterConnections: ClusterConnectionsType, excludedMasters: List[RedisURI])
                                (implicit executionContext: ExecutionContext): Future[NodeId]

  /**
    * Find the poorest master excluding the masters in the provide excluded List
    * @param excludedMasters Master to exclude
    * @param executionContext The execution context
    * @return Future containing the redis URI of the poorest remaining master
    */
  def findPoorestRemainingMaster(excludedMasters: List[RedisURI])
                                (implicit executionContext: ExecutionContext): Future[NodeId]

  /**
    * Replicate the master by the slave
    * @param newSlaveUri The Redis URI of the slave
    * @param masterNodeId The node Id of the master to be replicated
    * @param clusterConnections Cluster connections of master nodes
    * @param redisUriToNodeId mapping of Redis URI strings to node IDs
    * @param executionContext The execution context
    * @return Future Unit on success
    */
  def replicateMaster(newSlaveUri: RedisURI, masterNodeId: NodeId, clusterConnections: ClusterConnectionsType,
                      redisUriToNodeId: RedisUriToNodeId)
                     (implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Replicate the master by the slave
    * @param newSlaveUri The Redis URI of the slave
    * @param masterNodeId The node Id of the master to be replicated
    * @param executionContext The execution context
    * @return Future Unit on success
    */
  def replicateMaster(newSlaveUri: RedisURI, masterNodeId: NodeId)
                     (implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Retrieves the Redis cluster role of the Redis cluster node with the given URI
    * @param uri The uri of the Redis cluster node
    * @return Future of the Role of the cluster node, either Master or Slave
    */
  def getRole(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Role]

  /**
    * Fails over the master of a slave. The provided URI must be a URI of a slave node; the failover command is run on
    * this slave.
    * @param uri The URI of the slave to run the command on
    * @param executionContext The execution context
    * @return Future unit
    */
  def failoverMaster(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Fails over the master of a slave. The provided URI must be a URI of a master node. The slave of the given master
    * is found, and it then the failover command is run on this slave. If the given master does not have a slave, then
    * an exception is thrown: CornucopiaFailoverException.
    * @param uri The uri of the master that will be failed over by its slave
    * @param executionContext The execution context
    * @return Future unit
    */
  def failoverSlave(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Verify if a failover is successfull
    * @param uri The URI of the node to check
    * @param role The expected role of the node being checked
    * @return Future of boolean, true if it is correct, or false if not
    */
  def verifyFailover(uri: RedisURI, role: Role)(implicit executionContext: ExecutionContext): Future[Boolean]

  /**
    * Retrieve all the slaves of the given master
    * @param uri The RedisURI of the master to get slaves of
    * @param executionContext The execution context
    * @return Future of a List of the slave cluster nodes
    */
  def getSlavesOfMaster(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]]

  /**
    * Forget the redis node at the given URI
    * @param uri The URI of the redis node to forget
    * @param connections Cluster connections of master nodes
    * @param redisUriToNodeId mapping of Redis URI strings to node IDs
    * @param executionContext The execution context
    * @return Future Unit of success
    */
  def forgetNode(uri: RedisURI, connections: ClusterConnectionsType, redisUriToNodeId: RedisUriToNodeId)
                (implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Forget the redis node at the given URI
    * @param uri The URI of the redis node to forget
    * @param executionContext The execution context
    * @return Future Unit of success
    */
  def forgetNode(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * Get the cluster topology as it is
    * @param executionContext The execution context
    * @return Future on a map of cluster nodes, with keys for `masters` and `slaves`
    */
  def getClusterTopology(implicit executionContext: ExecutionContext): Future[Map[String, List[RedisClusterNode]]]

}

object ClusterOperationsImpl extends ClusterOperations {

  import ClusterOperations._

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * The entire cluster will meet the new node at the given URI.
    *
    * @param redisURI The URI of the new node
    * @param executionContext The thread dispatcher context.
    * @return The URI of the node that was added.
    */
  def addNodeToCluster(redisURI: RedisURI)(implicit executionContext: ExecutionContext): Future[RedisURI] = {
    val saladAPI = newSalad

    def getRedisConnection(nodeId: String): Future[Salad] = {
      implicit val salad = saladAPI()
      getConnection(nodeId).recoverWith {
        case e: RedisException =>
          saladAPI.shutdown()
          throw CornucopiaRedisConnectionException(s"Add nodes to cluster failed to get connection to node", e)
      }
    }

    saladAPI().clusterNodes.flatMap { allNodes =>
      val getConnectionsToLiveNodes = allNodes.filter(_.isConnected).map(node => getRedisConnection(node.getNodeId))
      Future.sequence(getConnectionsToLiveNodes).flatMap { connections =>
        val metResults = for {
          conn <- connections
        } yield {
          conn.clusterMeet(redisURI)
        }
        Future.sequence(metResults).map { _ =>
          saladAPI.shutdown()
          redisURI
        }
      }
    }

  }

  /**
    * Retrieves the source Redis nodes, which are the nodes that will give up keys to the new target master node
    * @param targetRedisURI The URI of the new master being added that will receive new key slots
    * @param executionContext Execution context
    * @return Future of a list of source nodes
    */
  def getRedisSourceNodes(targetRedisURI: RedisURI)
                         (implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]] = {
    val saladAPI = newSalad

    saladAPI().masterNodes.map { masters =>
      val masterNodes = masters.toList
      newSalad.shutdown()

      logger.debug(s"Reshard table with new master nodes: ${masterNodes.map(_.getNodeId)}")

      val liveMasters = masterNodes.filter(_.isConnected)

      logger.debug(s"Reshard cluster with new master live masters: ${liveMasters.map(_.getNodeId)}")

      val targetNode = masterNodes.filter(_.getUri == targetRedisURI).head

      logger.debug(s"Reshard cluster with new master target node: ${targetNode.getNodeId}")

      val sourceNodes = masterNodes.filterNot(_ == targetNode)

      logger.debug(s"Reshard cluster with new master source nodes: ${sourceNodes.map(_.getNodeId)}")

      sourceNodes
    } recover {
      case e =>
        throw CornucopiaGetRedisSourceNodesException("Could not get redis source nodes", e)
    }
  }

  def getRedisTargetNodesAndRetiredNode(retiredRedisURI: RedisURI)
                                       (implicit executionContext: ExecutionContext):
  Future[(List[RedisClusterNode], RedisClusterNode)] = {
    val saladAPI = newSalad

    saladAPI().masterNodes.map { masters =>
      val masterNodes = masters.toList
      saladAPI.shutdown()

      val liveMasters = masterNodes.filter(_.isConnected)

      val targetNodes = masterNodes.filterNot(_.getUri == retiredRedisURI)

      val retiredNode = masterNodes.filter(_.getUri == retiredRedisURI).head

      (targetNodes, retiredNode)
    }
  }

  /**
    * Gets a list of redis master nodes
    * @param executionContext
    * @return Future of a list of redis master nodes
    */
  def getRedisMasterNodes(implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]] = {
    val saladAPI = newSalad

    saladAPI().masterNodes.map { masters =>
      val masterNodes = masters.toList
      val liveMasters = masterNodes.filter(_.isConnected)
      saladAPI.shutdown()
      liveMasters
    }
  }

  /**
    * Retrieves the connections to the Redis cluster nodes
    * @param executionContext Execution context
    * @return Future of the cluster connections to master nodes
    */
  def getClusterConnections(implicit executionContext: ExecutionContext): Future[(ClusterConnectionsType, RedisUriToNodeId, SaladAPI)] = {

    implicit val saladAPI = newSalad

    val liveMasters: Future[List[RedisClusterNode]] = saladAPI().masterNodes.map { masters =>
      masters.toList.filter(_.isConnected)
    }

    implicit val salad = saladAPI()

    val connections: Future[List[(RedisClusterNode, Future[Connection.Salad])]] = for {
      masters <- liveMasters
    } yield {
      for {
        master <- masters
      } yield (master, getConnection(master.getNodeId))
    }

    val result: Future[List[((NodeId, RedisURI), Connection.Salad)]] = connections.flatMap { conns =>
      conns.unzip match {
        case (masters, futureConnections) =>
          val zero = List.empty[Connection.Salad]
          // foldLeft
          Future.fold(futureConnections)(zero) { (cs1, conn) =>
            cs1 ++ List(conn)
          } map { cs: List[Connection.Salad] =>
            masters.map(master => (master.getNodeId, master.getUri)).zip(cs)
          }
      }
    }

    val zero = (Map.empty[NodeId, Connection.Salad], Map.empty[RedisURI, NodeId])
    val results = result.map { cs =>
      cs.foldLeft(zero) { case ((connectionMap, uriMap), tuple) =>
        tuple match {
          case ((nodeId: NodeId, uri: RedisURI), conn: Connection.Salad) =>
            (connectionMap + (nodeId -> conn), uriMap + (uri -> nodeId))
        }
      }
    }
    results map { case (clusterConnections, uriToNodeId) => (clusterConnections, uriToNodeId, saladAPI) }
  }

  def isClusterReady(clusterConnections: ClusterConnectionsType)
                    (implicit executionContext: ExecutionContext): Future[Boolean] = {

    def isOk(info: Map[String,String]): Boolean = info("cluster_state") == "ok"

    val stateOfAllNodes: List[Future[Boolean]] = for {
      conn <- clusterConnections.values.toList
    } yield {
      for {
        info <- conn.clusterInfo
      } yield isOk(info)
    }

    Future.reduce(stateOfAllNodes)(_ && _)
  }

  def setSlotAssignment(slot: Slot, sourceNodeId: NodeId, targetNodeId: NodeId,
                        clusterConnections: ClusterConnectionsType)
                       (implicit executionContext: ExecutionContext): Future[Unit] = {

    val sourceConn = clusterConnections(sourceNodeId)
    val destinationConn = clusterConnections(targetNodeId)

    val result = (for {
      _ <- destinationConn.clusterSetSlotImporting(slot, sourceNodeId)
      _ <- sourceConn.clusterSetSlotMigrating(slot, targetNodeId)
    } yield {}) recover {
      case e => throw SetSlotAssignmentException(s"There was a problem setting slot assignment for slot $slot", e)
    }
    result
  }

  private def getConnectionForNode(clusterConnections: ClusterConnectionsType, nodeId: NodeId)
                                  (implicit executionContext: ExecutionContext): Future[Connection.Salad] = {
    Future {
      clusterConnections.get(nodeId) match {
        case Some(id) => id
        case None => throw CornucopiaRedisConnectionException("Problem getting redis connections")
      }
    }
  }

  def migrateSlotKeys(slot: Slot, targetRedisURI: RedisURI, sourceNodeId: NodeId, targetNodeId: NodeId,
                      clusterConnections: ClusterConnectionsType)
                     (implicit executionContext: ExecutionContext): Future[Unit] = {

    import com.adendamedia.salad.serde.ByteArraySerdes._

    val sourceConn = clusterConnections(sourceNodeId)

    // get all the keys in the given slot
    val keyList = for {
      keyCount <- sourceConn.clusterCountKeysInSlot(slot)
      keyList <- sourceConn.clusterGetKeysInSlot[CodecType](slot, keyCount.toInt)
    } yield keyList

    // migrate over all the keys in the slot from source to destination node
    val migrate = for {
      keys <- keyList
      result <- sourceConn.migrate[CodecType](targetRedisURI, keys.toList, timeout = 10000)
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
        throw MigrateSlotKeysBusyKeyException(reason = error)
      }
      else if (findError(errorString, "CLUSTERDOWN")) {
        throw MigrateSlotKeysClusterDownException(reason = error)
      }
      else if (findError(errorString, "MOVED")) {
        throw MigrateSlotKeysMovedException(reason = error)
      }
      else if (findError(errorString, "IOERR")) {
        throw MigrateSlotKeysInputOutputException(reason = error)
      }
      else {
        throw error
      }
    }

    migrate map  { _ =>
      logger.debug(s"Successfully migrated slot $slot from $sourceNodeId to $targetNodeId at ${targetRedisURI.getHost}:${targetRedisURI.getPort}")
    } recoverWith { case e => handleFailedMigration(e) }
  }

  /**
    * Notify all master nodes of a slot assignment so that they will immediately be able to redirect clients.
    */
  def notifySlotAssignment(slot: Slot, assignedNodeId: NodeId, clusterConnections: ClusterConnectionsType)
                          (implicit executionContext: ExecutionContext): Future[Unit] = {
    val notifications = clusterConnections map { case (_: NodeId, connection: Salad) =>
      connection.clusterSetSlotNode(slot, assignedNodeId) recover {
        case e: RedisCommandExecutionException =>
          logger.error(s"Problem notifying slot assignment: ", e)
          Unit
      }
    }
    Future.sequence(notifications).map(x => x)
  }

  def findPoorestMaster(clusterConnections: ClusterConnectionsType)
                       (implicit executionContext: ExecutionContext): Future[NodeId] = {

    // It might be overkill to get the slave nodes from every master connection
    val slaveNodes = clusterConnections map { case (nodeId, connection) =>
      connection.slaveNodes.map(_.toSet)
    }

    val slaves: Future[Set[RedisClusterNode]] =
      Future.fold(slaveNodes)(Set.empty[RedisClusterNode])((result, t) => result ++ t)

    val poorestMaster: Future[(NodeId, Int)] = slaves.map { slaveNodes =>
      slaveNodes.toList.flatMap { slave =>
        for {
          master <- Option(slave.getSlaveOf)
        } yield master
      }
    } map(_.groupBy(identity).mapValues(_.size)) map(_.min)

    poorestMaster recover {
      case e =>
        throw CornucopiaFindPoorestMasterException("Could not find a poorest master", e)
    }

    poorestMaster.map { case (nodeId, _) =>
      nodeId
    } recover {
      case e =>
        throw CornucopiaFindPoorestMasterException("Could not find a poorest master", e)
    }
  }

  def findPoorestMaster(implicit executionContext: ExecutionContext): Future[NodeId] = {
    getClusterConnections.flatMap { case (connections, _, salad) =>
      findPoorestMaster(connections) map { nodeId =>
        salad.shutdown()
        nodeId
      } recover {
        case e =>
          salad.shutdown()
          throw e
      }
    }
  }

  def findPoorestRemainingMaster(clusterConnections: ClusterConnectionsType, excludedMasters: List[RedisURI])
                                (implicit executionContext: ExecutionContext): Future[NodeId] = {
    // It might be overkill to get the slave nodes from every master connection
    val slaveNodes = clusterConnections map { case (nodeId, connection) =>
      connection.slaveNodes.map(_.toSet)
    }

    val slaves: Future[Set[RedisClusterNode]] =
      Future.fold(slaveNodes)(Set.empty[RedisClusterNode])((result, t) => result ++ t)

    val masterNodes = clusterConnections map { case (nodeId, connection) =>
      connection.masterNodes.map(_.toSet)
    }

    val masters: Future[Set[RedisClusterNode]] =
      Future.fold(masterNodes)(Set.empty[RedisClusterNode])((result, t) => result ++ t)

    val poorestRemainingMaster: Future[(NodeId, Int)] = masters.flatMap { masterNodes =>

      val excludedMasterUris: Set[RedisURI] = excludedMasters.toSet
      val excludedMasterNodeIds: Set[NodeId] =
        masterNodes.filter(node => excludedMasterUris.contains(node.getUri)).map(_.getNodeId)

      slaves.map { slaveNodes =>
        slaveNodes.toList.flatMap { slave =>
          for {
            master <- Option(slave.getSlaveOf) if !excludedMasterNodeIds.contains(master)
          } yield master
        }
      } map(_.groupBy(identity).mapValues(_.size)) map(_.min)
    }

    poorestRemainingMaster.map(_._1)
  }

  def findPoorestRemainingMaster(excludedMasters: List[RedisURI])
                                (implicit executionContext: ExecutionContext): Future[NodeId] = {

    getClusterConnections.flatMap { case (connections, _, salad) =>
      findPoorestRemainingMaster(connections, excludedMasters) map { x =>
        salad.shutdown()
        x
      } recover {
        case e =>
          salad.shutdown()
          throw e
      }
    }
  }

  def replicateMaster(newSlaveUri: RedisURI, masterNodeId: NodeId, clusterConnections: ClusterConnectionsType,
                      redisUriToNodeId: RedisUriToNodeId)
                     (implicit executionContext: ExecutionContext): Future[Unit] = {

    // Since when we join a node to the cluster it is first a master, then it should be in our collection of
    // cluster connections
    val newSlaveNodeId = redisUriToNodeId.getOrElse(
      newSlaveUri,
      throw ReplicateMasterException("Could not get new Redis node Id")
    )

    val newSlaveConnection = clusterConnections.getOrElse(
      newSlaveNodeId,
      throw ReplicateMasterException("Could not get new Redis slave connection")
    )

    newSlaveConnection.clusterReplicate(masterNodeId)
  }

  def replicateMaster(slaveUri: RedisURI, masterNodeId: NodeId)
                     (implicit executionContext: ExecutionContext): Future[Unit] = {
    val saladAPI = newSalad(slaveUri)
    saladAPI().clusterNodes.flatMap { clusterNodes =>
      val slaveNodeId = clusterNodes.toList.filter(node => node.getUri == slaveUri).map(_.getNodeId).headOption.getOrElse {
        saladAPI.shutdown()
        throw ReplicateMasterException(s"Could not get connection to slave node $slaveUri")
      }
      implicit val salad = saladAPI()
      getConnection(slaveNodeId) map { conn =>
        conn.clusterReplicate(masterNodeId).map(_ => saladAPI.shutdown())
      }
    }
  }

  def getRole(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Role] = {
    import com.lambdaworks.redis.models.role.RedisInstance.Role.{MASTER, SLAVE}
    implicit val saladAPI = newSalad

    val result = saladAPI().clusterNodes map { nodes =>
      for {
        node <- nodes.toList if node.getUri == uri
      } yield {
        saladAPI.shutdown()
        node.getRole match {
          case MASTER => Master
          case SLAVE => Slave
          case other =>
            throw CornucopiaGetRoleException(s"Invalid role: $other")
        }
      }
    } recover {
      case e: Throwable =>
        saladAPI.shutdown()
        throw CornucopiaGetRoleException(s"Something bad happened", e)
    }

    result.map(_.headOption.getOrElse(throw CornucopiaGetRoleException(s"Could not get role")))
  }

  def failoverMaster(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit] = {
    val saladAPI = newSalad(uri)
    implicit val salad = saladAPI()

    getConnection(uri).flatMap(_.clusterFailover() map(_ => saladAPI.shutdown())) recover { case e =>
      saladAPI.shutdown()
      throw CornucopiaFailoverException(s"Could not fail over slave $uri", e)
    }
  }

  def failoverSlave(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit] = {
    implicit val saladAPI = newSalad

    saladAPI().clusterNodes map { clusterNodes =>
      val nodes = clusterNodes.toList

      val masterNodeId: NodeId = nodes.find(_.getUri == uri).getOrElse(
        throw CornucopiaFailoverException(s"Could not find node with uri $uri")
      ).getNodeId

      val slaveNodeId: NodeId = nodes.find(_.getSlaveOf == masterNodeId).getOrElse(
        throw CornucopiaFailoverException(s"Could not get slave node to fail over master node $uri")
      ).getNodeId

      implicit val salad = saladAPI()

      getConnection(slaveNodeId).flatMap(_.clusterFailover() map(_ => saladAPI.shutdown())) recover { case e =>
        saladAPI.shutdown()
        throw CornucopiaFailoverException(s"Could not fail over slave $slaveNodeId of master $uri", e)
      }
    }
  }

  def verifyFailover(uri: RedisURI, role: Role)(implicit executionContext: ExecutionContext): Future[Boolean] = {
    getRole(uri) map(_ == role)
  }

  def getSlavesOfMaster(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]] = {
    implicit val saladAPI = newSalad

    saladAPI().clusterNodes map { clusterNodes =>
      val nodes = clusterNodes.toList
      saladAPI.shutdown()
      for {
        master <- nodes if master.getUri == uri
        slave <- nodes if slave.getSlaveOf == master.getNodeId
      } yield slave
    }
  }

  def forgetNode(uri: RedisURI, connections: ClusterConnectionsType, redisUriToNodeId: RedisUriToNodeId)
                (implicit executionContext: ExecutionContext): Future[Unit] = {

    val nodeToRemove: (NodeId, Salad) = (for {
      nodeId <- redisUriToNodeId.get(uri)
      connection <- connections.get(nodeId)
    } yield nodeId -> connection).getOrElse(
      throw CornucopiaForgetNodeException(s"Could not find the connection to redis node $uri")
    )

    val (removeNodeId, removeConnection) = nodeToRemove
    val remainingNodes: List[Salad] = connections.filterKeys(_ != removeNodeId).values.toList

    removeConnection.clusterReset(hard = true).flatMap { _ =>
      val results: List[Future[Unit]] = for {
        node <- remainingNodes
      } yield node.clusterForget(removeNodeId) map identity
      Future.fold(results)()((r, _) => r)
    } recover {
      case e: RedisCommandExecutionException => throw e // This isn't likely to happen
    }

  }

  def forgetNode(uri: RedisURI)(implicit executionContext: ExecutionContext): Future[Unit] = {
    implicit val saladAPI = newSalad

    saladAPI().clusterNodes map { clusterNodes =>
      val nodes = clusterNodes.toList
      implicit val salad = saladAPI()

      val nodeIdToRemove = nodes.find(_.getUri == uri).map(_.getNodeId).getOrElse(
        throw CornucopiaForgetNodeException(s"Could not find the node id of redis node $uri to remove")
      )

      val remainingConnections = nodes.filterNot(_.getNodeId == nodeIdToRemove) map(node => getConnection(node.getNodeId))

      getConnection(nodeIdToRemove).flatMap { conn =>
        conn.clusterReset(hard = true).flatMap { _ =>
          Future.sequence(remainingConnections).map { connections =>
            connections.map { conn =>
              conn.clusterForget(nodeIdToRemove).map(identity)
            }
          } map (_ => saladAPI.shutdown())
        }
      } recover {
        case e: RedisCommandExecutionException =>
          saladAPI.shutdown()
          throw e // This isn't likely to happen
      }
    }
  }

  def getClusterTopology(implicit executionContext: ExecutionContext): Future[Map[String, List[RedisClusterNode]]] = {
    import com.lambdaworks.redis.models.role.RedisInstance.Role.{MASTER, SLAVE}
    implicit val saladAPI = newSalad

    saladAPI().clusterNodes map { allNodes =>
      val masterNodes = allNodes.filter(MASTER == _.getRole)
      val slaveNodes = allNodes.filter(SLAVE == _.getRole)
      saladAPI.shutdown()

      Map("masters" -> masterNodes.toList, "slaves" -> slaveNodes.toList)
    }
  }

}
