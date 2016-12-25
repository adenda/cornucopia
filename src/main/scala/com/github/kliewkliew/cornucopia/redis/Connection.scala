package com.github.kliewkliew.cornucopia.redis

import java.net.InetAddress

import com.github.kliewkliew.salad.api.async.{AsyncSaladAPI, SaladClusterAPI}
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.api.async.{RedisAdvancedClusterAsyncCommands, RedisClusterAsyncCommands}
import com.lambdaworks.redis.cluster.RedisClusterClient
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object Connection {
  // Initialize the configuration.
  private val redisConfig = ConfigFactory.load().getConfig("redis")
  private val redisClusterConfig = redisConfig.getConfig("cluster")
  private val redisClusterSeedServer = redisClusterConfig.getString("seed.server")
  private val redisClusterPort = redisClusterConfig.getInt("server.port")

  // Initialize the API.
  private val nodes = List(RedisURI.create(redisClusterSeedServer, redisClusterPort))

  /**
    * Create a new API connection - new connections are necessary to refresh the view of the cluster topology
    * after adding or removing a node.
    * To reuse the same connection, assign it to a val and pass explicitly or as an implicit parameter.
    */
  type SaladAPI = AsyncSaladAPI[String,String,RedisAdvancedClusterAsyncCommands[String,String]]
  def newSaladAPI: SaladAPI = AsyncSaladAPI(RedisClusterClient.create(nodes.asJava).connect().async())

  /**
    * Get a connection to one node in the cluster.
    * @param redisURI
    * @return
    */
  def getConnection(redisURI: RedisURI)(implicit saladAPI: SaladAPI): SaladClusterAPI[String,String] =
    verifyConnection(
      Try(saladAPI.underlying.getConnection(
        InetAddress.getByName(redisURI.getHost).getHostAddress,
        redisURI.getPort)),
      redisURI.toString)
  def getConnection(nodeId: String)(implicit saladAPI: SaladAPI): SaladClusterAPI[String,String] =
    verifyConnection(
      Try(saladAPI.underlying.getConnection(nodeId)),
      nodeId)
  def verifyConnection(api: Try[RedisClusterAsyncCommands[String,String]], identifier: String) =
  api match {
    case Success(conn) =>
      SaladClusterAPI(conn)
    case Failure(e) =>
      val err = s"Failed to connect to node: $identifier"
      LoggerFactory.getLogger(this.getClass).error(err, e)
      throw new Exception(err)
  }

}
