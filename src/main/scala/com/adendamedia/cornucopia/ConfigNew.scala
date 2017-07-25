package com.adendamedia.cornucopia

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext
import com.adendamedia.cornucopia.actors.SharedActorSystem.sharedActorSystem

object ConfigNew {

  trait JoinRedisNodeConfig {
    val maxNrRetries: Int
  }

  trait ReshardClusterConfig {
    val maxNrRetries: Int
    val expectedTotalNumberSlots: Int
    val executionContext: ExecutionContext
  }

  trait ClusterConnectionsConfig {
    /**
      * The maximum number of retries to try and get cluster connections
      */
    val maxNrRetries: Int
    val executionContext: ExecutionContext
    val expectedTotalNumberSlots: Int
  }

  trait ClusterReadyConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int

    /**
      * Time in seconds to wait before checking if cluster is ready yet
      */
    val backOffTime: Int
  }

  trait MigrateSlotsConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
    val numberOfWorkers: Int
  }

  trait ReplicatePoorestMasterConfig {
    val executionContext: ExecutionContext
  }

}

class ConfigNew {
  import ConfigNew._

  implicit val actorSystem: ActorSystem = sharedActorSystem

  object ReshardTableConfig {
    final implicit val ExpectedTotalNumberSlots: Int = 16384
  }

  object Cornucopia {
    private val config = ConfigFactory.load().getConfig("cornucopia")

    object JoinRedisNode extends JoinRedisNodeConfig {
      val maxNrRetries: Int = config.getInt("join.node.max.retries")
    }

    object ReshardCluster extends ReshardClusterConfig {
      val maxNrRetries: Int = config.getInt("reshard.cluster.max.retries")
      val expectedTotalNumberSlots: Int = ReshardTableConfig.ExpectedTotalNumberSlots
      val executionContext: ExecutionContext = actorSystem.dispatcher
    }

    object ClusterConnections extends ClusterConnectionsConfig {
      val maxNrRetries: Int = config.getInt("cluster.connections.max.retries")
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val expectedTotalNumberSlots: Int = ReshardTableConfig.ExpectedTotalNumberSlots
    }

    object ClusterReady extends ClusterReadyConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("cluster.ready.max.retries")
      val backOffTime: Int = config.getInt("cluster.ready.backoff.time")
    }

    object MigrateSlots extends MigrateSlotsConfig {
      val executionContext: ExecutionContext = actorSystem.dispatchers.lookup("akka.actor.migrate-slots-dispatcher")
      val maxNrRetries: Int = config.getInt("migrate.slots.max.retries")
      val numberOfWorkers: Int = config.getInt("migrate.slots.workers")
    }

    object ReplicatePoorestMaster extends ReplicatePoorestMasterConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
    }

  }

}
