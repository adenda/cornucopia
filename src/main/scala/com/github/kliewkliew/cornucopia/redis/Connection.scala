package com.github.kliewkliew.cornucopia.redis

import java.net.URI
import java.util.concurrent.TimeUnit

import com.github.kliewkliew.salad.api.async.FutureConverters._
import com.github.kliewkliew.salad.api.async.AsyncSaladAPI
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.api.async.RedisClusterAsyncCommands
import com.lambdaworks.redis.cluster.models.partitions.{ClusterPartitionParser, RedisClusterNode}
import com.lambdaworks.redis.cluster.{ClusterClientOptions, ClusterTopologyRefreshOptions, RedisClusterClient}
import com.lambdaworks.redis.models.role.RedisInstance.Role
import com.typesafe.config.ConfigFactory

import collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Connection {
  // Initialize the configuration.
  private val redisConfig = ConfigFactory.load().getConfig("redis")
  private val redisClusterConfig = redisConfig.getConfig("cluster")
  private val redisClusterSeedServers = redisClusterConfig.getStringList("seed.servers")
  private val redisClusterPort = redisClusterConfig.getInt("server.port")
  private val redisClusterRefreshInterval = redisClusterConfig.getInt("refresh.interval")

  // Initialize the API.
  private val nodes = redisClusterSeedServers.asScala.map(RedisURI.create(_, redisClusterPort))
  private val clusterClient = RedisClusterClient.create(nodes.asJava)
  private val connection = clusterClient.connect()
  private val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
    .enablePeriodicRefresh(redisClusterRefreshInterval, TimeUnit.MINUTES)
    .build()
  clusterClient.setOptions(ClusterClientOptions.builder()
    .topologyRefreshOptions(topologyRefreshOptions)
    .build())
  private val lettuceAPI = connection.async()

  /**
    * The API used to contact the cluster.
    */
  val saladAPI = AsyncSaladAPI(lettuceAPI)

  /**
    * Get the information of one node in the cluster.
    * @param uRI
    * @return
    */
  def node(uRI: URI): RedisClusterNode = {
    val node = new RedisClusterNode
    node.setUri(RedisURI.create(uRI.getHost, uRI.getPort))
    node
  }

  /**
    * Get a connection to one node in the cluster.
    * @param uRI
    * @return
    */
  def connection(uRI: URI): RedisClusterAsyncCommands[String,String] =
    saladAPI.underlying.getConnection(uRI.getHost, uRI.getPort)

  /**
    * Get a list of all nodes in the cluster.
    * @return
    */
  def clusterNodes: Future[mutable.Buffer[RedisClusterNode]] = saladAPI.underlying.clusterNodes.map { response =>
    ClusterPartitionParser.parse(response).getPartitions.asScala
  }

  /**
    * Get a list of master nodes in the cluster.
    * @return
    */
  def masterNodes: Future[mutable.Buffer[RedisClusterNode]] =
    clusterNodes.map(_.filter(Role.MASTER == _.getRole))
}
