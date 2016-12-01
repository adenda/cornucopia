package com.github.kliewkliew.cornucopia.redis

import java.util.concurrent.TimeUnit

import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.{ClusterClientOptions, ClusterTopologyRefreshOptions, RedisClusterClient}
import com.lambdaworks.redis.codec.ByteArrayCodec
import com.typesafe.config.ConfigFactory

import collection.JavaConverters._

class Connection {
  // Initialize the configuration.
  private val redisConfig = ConfigFactory.load().getConfig("redis")
  private val redisClusterConfig = redisConfig.getConfig("cluster")
  private val redisClusterSeedServers = redisClusterConfig.getStringList("seed.servers")
  private val redisClusterPort = redisClusterConfig.getInt("server.port")
  private val redisClusterRefreshInterval = redisClusterConfig.getInt("refresh.interval")

  // Initialize the API.
  private val nodes = redisClusterSeedServers.asScala.map(RedisURI.create(_, redisClusterPort))
  private val clusterClient = RedisClusterClient.create(nodes.asJava)
  private val connection = clusterClient.connect(ByteArrayCodec.INSTANCE)
  private val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
    .enablePeriodicRefresh(redisClusterRefreshInterval, TimeUnit.MINUTES)
    .build()
  clusterClient.setOptions(ClusterClientOptions.builder()
    .topologyRefreshOptions(topologyRefreshOptions)
    .build())

  private val lettuceAPI = connection.sync()
}
