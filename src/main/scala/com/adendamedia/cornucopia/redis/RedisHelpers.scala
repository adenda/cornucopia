package com.adendamedia.cornucopia.redis

import org.slf4j.LoggerFactory
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.lambdaworks.redis.RedisURI

import scala.util.Try

object RedisHelpers {

  @SerialVersionUID(1L)
  case class RedisClusterConnectionsInvalidException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends RuntimeException(message, cause) with Serializable

}

trait RedisHelpers {
  import RedisHelpers._
  import ClusterOperations._

  /**
    * Checks if the nodes list contains all the masters, and then checks that all those masters are in the connections
    * Tuple
    * @param nodes Redis cluster nodes to count the hash slots in
    * @param connections The connections we are checking if they are valid or not
    * @return
    */
  def compareUsingSlotsCount(nodes: List[RedisClusterNode],
                             connections: (ClusterConnectionsType, RedisUriToNodeId))
                            (implicit expectedTotalNumberSlots: Int): Boolean

  /**
    * Checks if connections object contains a connection to provided redis uri
    * @param redisUri the redis uri to check if it has connections
    * @param connections the connections to check in
    * @return true, otherwise throws an exception
    */
  def connectionsHaveRedisNode(redisUri: RedisURI, connections: (ClusterConnectionsType, RedisUriToNodeId)): Boolean
}

object RedisHelpersImpl extends RedisHelpers {
  import ClusterOperations._
  import ReshardTableNew._
  import RedisHelpers._

  def compareUsingSlotsCount(nodes: List[RedisClusterNode],
                             connections: (ClusterConnectionsType, RedisUriToNodeId))
                            (implicit expectedTotalNumberSlots: Int): Boolean = {
    import scala.collection.JavaConverters._

    val logicalNodes = nodes.map { n =>
      val slots = n.getSlots.asScala.toList.map(_.toInt)
      LogicalNode(n, slots)
    }

    val totalSlots = logicalNodes.foldLeft(0)((sum, n) => sum + n.slots.size)

    if (totalSlots != expectedTotalNumberSlots)
      throw RedisClusterConnectionsInvalidException(s"Total slots is $totalSlots, but is not equal to expected number $expectedTotalNumberSlots")

    if(connections._1.size < logicalNodes.size)
      throw RedisClusterConnectionsInvalidException(s"Too few connections found. Expected number is ${logicalNodes.size}, actual number is ${connections._1.size}")

    true
  }

  def connectionsHaveRedisNode(redisUri: RedisURI,
                               connections: (ClusterConnectionsType, RedisUriToNodeId)): Boolean = {

    val nodeId = connections._2.getOrElse(
      redisUri.toString,
      throw RedisClusterConnectionsInvalidException("Redis uri missing from connections")
    )

    connections._1.getOrElse(
      nodeId,
      throw RedisClusterConnectionsInvalidException(s"Redis connection for uri ${redisUri.toString} missing from connections")
    )
    
    true
  }
}
