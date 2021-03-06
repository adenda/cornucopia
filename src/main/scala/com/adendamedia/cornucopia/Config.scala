package com.adendamedia.cornucopia

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext

object Config {

  trait JoinRedisNodeConfig {
    val maxNrRetries: Int
    val refreshTimeout: Int
    val retryBackoffTime: Int
    val executionContext: ExecutionContext
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
    val retryBackoffTime: Int
  }

  trait ClusterReadyConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
    val clusterReadyRetries: Int

    /**
      * Time in seconds to wait before checking if cluster is ready yet
      */
    val backOffTime: Int
  }

  trait MigrateSlotsConfig {
    val executionContext: ExecutionContext
    // maximum number of migrate slot jobs that can fail before the entire migrates slots jobs has to fail
    val failureThreshold: Int
    val maxNrRetries: Int
    val numberOfWorkers: Int
    val setSlotAssignmentRetryBackoff: Int
    val notifySlotAssignmentRetryBackoff: Int
  }

  trait ReplicatePoorestMasterConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
  }

  trait FailoverConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
    val verificationRetryBackOffTime: Int
    val maxNrAttemptsToVerify: Int
    val refreshTimeout: Int
  }

  trait ForgetRedisNodeConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
    val refreshTimeout: Int
  }

  trait GetSlavesOfMasterConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
  }

  trait ClusterTopologyConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int
  }

}

class Config(implicit val sharedActorSystem: ActorSystem) {
  import Config._

  implicit val actorSystem: ActorSystem = sharedActorSystem

  object ReshardTableConfig {
    final implicit val ExpectedTotalNumberSlots: Int = 16384
  }

  object Cornucopia {
    private val config = ConfigFactory.load().getConfig("cornucopia")

    object JoinRedisNode extends JoinRedisNodeConfig {
      val maxNrRetries: Int = config.getInt("join.node.max.retries")
      val refreshTimeout: Int = config.getInt("join.node.refresh.timeout")
      val retryBackoffTime: Int = config.getInt("join.node.retry.backoff.time")
      val executionContext: ExecutionContext = actorSystem.dispatcher
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
      val retryBackoffTime: Int = config.getInt("cluster.connections.retry.backoff.time")
    }

    object ClusterReady extends ClusterReadyConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("cluster.ready.max.retries")
      val backOffTime: Int = config.getInt("cluster.ready.backoff.time")
      val clusterReadyRetries: Int = config.getInt("cluster.ready.retries")
    }

    object MigrateSlots extends MigrateSlotsConfig {
      val executionContext: ExecutionContext = actorSystem.dispatchers.lookup("akka.actor.migrate-slots-dispatcher")
      val maxNrRetries: Int = config.getInt("migrate.slots.max.retries")
      val failureThreshold: Int = config.getInt("migrate.slots.failure.threshold")
      val numberOfWorkers: Int = config.getInt("migrate.slots.workers")
      val setSlotAssignmentRetryBackoff: Int = config.getInt("migrate.slots.set.slot.assignment.retry.backoff")
      val notifySlotAssignmentRetryBackoff: Int = config.getInt("migrate.slots.notify.slot.assignment.retry.backoff")
    }

    object ReplicatePoorestMaster extends ReplicatePoorestMasterConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("replicate.poorest.master.max.retries")
    }

    object Failover extends FailoverConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("failover.max.retries")
      val verificationRetryBackOffTime: Int = config.getInt("failover.verification.retry.backoff.time")
      val maxNrAttemptsToVerify: Int = config.getInt("failover.max.attempts.to.verify")
      val refreshTimeout: Int = config.getInt("failover.refresh.timeout")
    }

    object GetSlavesOfMaster extends GetSlavesOfMasterConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("get.slaves.of.master.max.retries")
    }

    object ForgetRedisNode extends ForgetRedisNodeConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("forget.redis.nodes.max.retries")
      val refreshTimeout: Int = config.getInt("forget.redis.nodes.refresh.timeout")
    }

    object ClusterTopology extends ClusterTopologyConfig {
      val executionContext: ExecutionContext = actorSystem.dispatcher
      val maxNrRetries: Int = config.getInt("cluster.topology.max.retries")
    }

  }

}
