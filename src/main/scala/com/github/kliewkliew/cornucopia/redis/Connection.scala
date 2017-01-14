package com.github.kliewkliew.cornucopia.redis

import java.net.InetAddress

import com.github.kliewkliew.salad.api.async.{AsyncSaladAPI, SaladClusterAPI}
import com.lambdaworks.redis.{ReadFrom, RedisURI}
import com.lambdaworks.redis.cluster.api.async.{RedisAdvancedClusterAsyncCommands, RedisClusterAsyncCommands}
import com.lambdaworks.redis.cluster.{ClusterClientOptions, ClusterTopologyRefreshOptions, RedisClusterClient}
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Connection {
  // Initialize the configuration.
  private val redisConfig = ConfigFactory.load().getConfig("redis")
  private val redisClusterConfig = redisConfig.getConfig("cluster")
  private val redisClusterSeedServer = redisClusterConfig.getString("seed.server.host")
  private val redisClusterPort = redisClusterConfig.getInt("seed.server.port")

  // Initialize the API.
  private val nodes = List(RedisURI.create(redisClusterSeedServer, redisClusterPort))

  /**
    * Create a new API connection - new connections are necessary to refresh the view of the cluster topology
    * after adding or removing a node.
    * Also, resharding requires tens of thousands of API calls. The OS won't handle so many open connections due to file
    * handle limits.
    * To reuse the same connection, assign it to a val and pass explicitly or as an implicit parameter.
    */
  type CodecType = Array[Byte]
  type SaladAPI = AsyncSaladAPI[CodecType,CodecType,RedisAdvancedClusterAsyncCommands[CodecType,CodecType]]
  def newSaladAPI: SaladAPI = newSaladAPI(nodes)
  def newSaladAPI(redisURI: RedisURI): SaladAPI = newSaladAPI(List(redisURI))
  def newSaladAPI(redisURI: List[RedisURI]): SaladAPI = {
    val client = RedisClusterClient.create(redisURI.asJava)
    val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
      .enableAllAdaptiveRefreshTriggers()
      .build()
    client.setOptions(ClusterClientOptions.builder()
      .topologyRefreshOptions(topologyRefreshOptions)
      .build())
    val connection = client.connect(ByteArrayCodec.INSTANCE)
    connection.setReadFrom(ReadFrom.MASTER)
    AsyncSaladAPI(connection.async())
  }

  /**
    * Get a connection to one node in the cluster.
    * @param redisURI
    * @return
    */
  def getConnection(redisURI: RedisURI)(implicit saladAPI: SaladAPI, executionContext: ExecutionContext)
  : Future[SaladClusterAPI[CodecType,CodecType]] =
    verifyConnection(
      Try(saladAPI.underlying.getConnection(
        saladAPI.canonicalizeURI(redisURI).getHost,
        redisURI.getPort)),
      redisURI.toString)
  def getConnection(nodeId: String)(implicit saladAPI: SaladAPI, executionContext: ExecutionContext)
  : Future[SaladClusterAPI[CodecType,CodecType]] =
    verifyConnection(
      Try(saladAPI.underlying.getConnection(nodeId)),
      nodeId)
  def verifyConnection(api: Try[RedisClusterAsyncCommands[CodecType,CodecType]], identifier: String)
                      (implicit executionContext: ExecutionContext)
  : Future[SaladClusterAPI[CodecType,CodecType]] =
  api match {
    case Success(conn) =>
      Future(SaladClusterAPI(conn))
    case Failure(e) =>
      val err = s"Failed to connect to node: $identifier"
      LoggerFactory.getLogger(this.getClass).error(err, e)
      Future.failed(e)
  }

}
