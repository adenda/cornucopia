package com.adendamedia.cornucopia.redis

//import com.adendamedia.cornucopia.CornucopiaException._
import org.slf4j.LoggerFactory
import com.adendamedia.cornucopia.redis.Connection._
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.adendamedia.salad.SaladClusterAPI

import scala.concurrent.{ExecutionContext, Future, blocking}
import com.lambdaworks.redis.{RedisException, RedisURI}

trait ClusterOperations {
  def addNodeToCluster(redisURI: RedisURI)(implicit executionContext: ExecutionContext): Future[RedisURI]

  def getRedisSourceNodes(targetRedisURI: RedisURI)
                         (implicit executionContext: ExecutionContext): Future[List[RedisClusterNode]]
}

trait ClusterOperationsExceptions {

  @SerialVersionUID(1L)
  class CornucopiaRedisConnectionException(msg: String, reason: Throwable = None.orNull)
    extends Throwable(msg: String, reason: Throwable) with Serializable
}

object ClusterOperationsImpl extends ClusterOperations with ClusterOperationsExceptions {

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
        case e: RedisException => throw new CornucopiaRedisConnectionException(s"Add nodes to cluster failed to get connection to node", e)
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

}
