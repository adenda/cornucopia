package com.adendamedia.cornucopia.redis

//import com.adendamedia.cornucopia.CornucopiaException._
import org.slf4j.LoggerFactory
import com.adendamedia.cornucopia.redis.Connection._
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.salad.SaladClusterAPI

import scala.concurrent.{ExecutionContext, Future, blocking}
import com.lambdaworks.redis.{RedisException, RedisURI}

object ClusterOperations {

  @SerialVersionUID(1L)
  case class CornucopiaRedisConnectionException(message: String, reason: Throwable = None.orNull)
    extends Throwable(message, reason) with Serializable

  type NodeId = String
  type RedisUriString = String
  type ClusterConnectionsType = Map[NodeId, Connection.Salad]
  type RedisUriToNodeId = Map[RedisUriString, NodeId]
}

trait ClusterOperations {
  import ClusterOperations._

  def addNodeToCluster(redisURI: RedisURI)(implicit executionContext: ExecutionContext): Future[RedisURI]

  def getRedisSourceNodes(targetRedisURI: RedisURI)
                         (implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]]

  def getClusterConnections(implicit executionContext: ExecutionContext): Future[(ClusterConnectionsType, RedisUriToNodeId)]

  /**
    * Checks if the cluster status of all Redis node connections is "OK"
    * @param clusterConnections The connections to cluster nodes (masters)
    * @param executionContext The Execution context
    * @return Future Boolean, true if all nodes are OK, false otherwise
    */
  def isClusterReady(clusterConnections: ClusterConnectionsType)
                    (implicit executionContext: ExecutionContext): Future[Boolean]

  def migrateSlot(slot: Slot, sourceNodeId: NodeId, targetNodeId: NodeId, clusterConnections: ClusterConnectionsType)
                 (implicit executionContext: ExecutionContext): Future[Unit]
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
    implicit val saladAPI = newSaladAPI

    def getRedisConnection(nodeId: String): Future[Salad] = {
      getConnection(nodeId).recoverWith {
        case e: RedisException => throw CornucopiaRedisConnectionException(s"Add nodes to cluster failed to get connection to node", e)
      }
    }

    saladAPI.clusterNodes.flatMap { allNodes =>
      val getConnectionsToLiveNodes = allNodes.filter(_.isConnected).map(node => getRedisConnection(node.getNodeId))

      Future.sequence(getConnectionsToLiveNodes).flatMap { connections =>
        val metResults = for {
          conn <- connections
        } yield {
          conn.clusterMeet(redisURI)
        }
        Future.sequence(metResults).map(_ => redisURI)
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
    val saladAPI = newSaladAPI

    saladAPI.masterNodes.map { masters =>
      val masterNodes = masters.toList

      logger.debug(s"Reshard table with new master nodes: ${masterNodes.map(_.getNodeId)}")

      val liveMasters = masterNodes.filter(_.isConnected)

      logger.debug(s"Reshard cluster with new master live masters: ${liveMasters.map(_.getNodeId)}")

      val targetNode = masterNodes.filter(_.getUri == targetRedisURI).head

      logger.debug(s"Reshard cluster with new master target node: ${targetNode.getNodeId}")

      val sourceNodes = masterNodes.filterNot(_ == targetNode)

      logger.debug(s"Reshard cluster with new master source nodes: ${sourceNodes.map(_.getNodeId)}")

      sourceNodes
    }
  }

  /**
    * Retrieves the connections to the Redis cluster nodes
    * @param executionContext Execution context
    * @return Future of the cluster connections to master nodes
    */
  def getClusterConnections(implicit executionContext: ExecutionContext): Future[(ClusterConnectionsType, RedisUriToNodeId)] = {

    implicit val saladAPI = newSaladAPI

    val liveMasters: Future[List[RedisClusterNode]] = saladAPI.masterNodes.map { masters =>
      masters.toList.filter(_.isConnected)
    }

    val connections: Future[List[(RedisClusterNode, Future[Connection.Salad])]] = for {
      masters <- liveMasters
    } yield {
      for {
        master <- masters
      } yield (master, getConnection(master.getNodeId))
    }

    val result: Future[List[((NodeId, RedisUriString), Connection.Salad)]] = connections.flatMap { conns =>
      conns.unzip match {
        case (masters, futureConnections) =>
          val zero = List.empty[Connection.Salad]
          // foldLeft
          Future.fold(futureConnections)(zero) { (cs1, conn) =>
            cs1 ++ List(conn)
          } map { cs: List[Connection.Salad] =>
            masters.map(master => (master.getNodeId, master.getUri.toString)).zip(cs)
          }
      }
    }

    val zero = (Map.empty[NodeId, Connection.Salad], Map.empty[RedisUriString, NodeId])
    result.map { cs =>
      cs.foldLeft(zero) { case ((connectionMap, uriMap), tuple) =>
        tuple match {
          case ((nodeId: NodeId, uri: RedisUriString), conn: Connection.Salad) =>
            (connectionMap + (nodeId -> conn), uriMap + (uri -> nodeId))
        }
      }
    }
  }

  def isClusterReady(clusterConnections: ClusterConnectionsType)
                    (implicit executionContext: ExecutionContext): Future[Boolean] = {
    Future(true) // TODO
  }

  def migrateSlot(slot: Slot, sourceNodeId: NodeId, targetNodeId: NodeId, clusterConnections: ClusterConnectionsType)
    (implicit executionContext: ExecutionContext): Future[Unit] = {
    Future(Unit)
  }

}
