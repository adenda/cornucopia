package com.adendamedia.cornucopia.redis

import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.Connection._

import scala.concurrent.{ExecutionContext, Future, blocking}
import com.lambdaworks.redis.{RedisException, RedisURI}

object ClusterOperations {

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

}
