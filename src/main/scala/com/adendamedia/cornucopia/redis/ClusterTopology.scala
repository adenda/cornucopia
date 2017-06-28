package com.adendamedia.cornucopia.redis

import com.adendamedia.cornucopia.redis._
import com.adendamedia.cornucopia.redis.Connection.{CodecType, Salad, getConnection, newSaladAPI}
import com.adendamedia.salad.SaladClusterAPI

import org.slf4j.LoggerFactory
import com.lambdaworks.redis.{RedisException, RedisURI}
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode
import com.lambdaworks.redis.models.role.RedisInstance.Role
import scala.concurrent.{ExecutionContext, Future}

/**
  * Return an ADT of the entire Redis cluster topology
  */
object ClusterTopology {

  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def getNewSaladApi: Salad = newSaladAPI

  def logTopology(implicit executionContext: ExecutionContext): Future[Unit] = {
    implicit val saladAPI = getNewSaladApi
    saladAPI.clusterNodes.map { allNodes =>
      val masterNodes = allNodes.filter(Role.MASTER == _.getRole)
      val slaveNodes = allNodes.filter(Role.SLAVE == _.getRole)
      logger.info(s"Master nodes: $masterNodes")
      logger.info(s"Slave nodes: $slaveNodes")
    }
  }

  /**
    * Returns the cluster topology as a Buffer of RedisClusterNode
    * @param executionContext
    * @return The cluster topology wrapped in a Future
    */
  def getClusterTopology(implicit executionContext: ExecutionContext): Future[scala.collection.mutable.Buffer[RedisClusterNode]] = {
    implicit val saladAPI = getNewSaladApi
    saladAPI.clusterNodes
  }

}
