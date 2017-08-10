package com.adendamedia.cornucopia

import akka.actor.{ActorRef, ActorRefFactory, Props, ActorSystem}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ClusterOperationsImpl}
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis._

object Cornucopia {
  import ConfigNew._

  trait JoinRedisNode {
    implicit val joinRedisNodeConfig: JoinRedisNodeConfig
    implicit val clusterOperations: ClusterOperations
    val factory: ActorRefFactory => ActorRef
  }

  trait ReshardCluster {
    implicit val reshardClusterConfig: ReshardClusterConfig
    implicit val clusterOperations: ClusterOperations
    val factory: ActorRefFactory => ActorRef
  }

  trait ClusterConnections {
    implicit val clusterConnectionsConfig: ClusterConnectionsConfig
    implicit val clusterOperations: ClusterOperations
    val factory: ActorRefFactory => ActorRef
  }

  trait ClusterReady {
    implicit val clusterReadyConfig: ClusterReadyConfig
    implicit val clusterOperations: ClusterOperations
    val factory: ActorRefFactory => ActorRef
  }

  trait MigrateSlots {
    implicit val migrateSlotsConfig: MigrateSlotsConfig
    implicit val clusterOperations: ClusterOperations
    val factory: ActorRefFactory => ActorRef
  }

  trait ReplicatePoorestMaster {
    implicit val replicatePoorestMasterConfig: ReplicatePoorestMasterConfig
    implicit val clusterOperations: ClusterOperations
  }

  trait Failover {
    implicit val failoverConfig: FailoverConfig
    implicit val clusterOperations: ClusterOperations
  }

  trait GetSlavesOfMaster {
    implicit val getSlavesOfMasterConfig: GetSlavesOfMasterConfig
    implicit val clusterOperations: ClusterOperations
  }

  trait ForgetRedisNode {
    implicit val forgetRedisNodeConfig: ForgetRedisNodeConfig
    implicit val clusterOperations: ClusterOperations
  }

  trait ClusterTopology {
    implicit val clusterTopologyConfig: ClusterTopologyConfig
    implicit val clusterOperations: ClusterOperations
  }

}

class Cornucopia(implicit val sharedActorSystem: ActorSystem) {
  import Cornucopia._
  import ConfigNew._

  private val config = new ConfigNew

  private val system: ActorSystem = config.actorSystem

  private val clusterOperationsImpl: ClusterOperations = ClusterOperationsImpl

  private val redisHelpersImpl: RedisHelpers = RedisHelpersImpl

  object JoinRedisNodeImpl extends JoinRedisNode {
    implicit val joinRedisNodeConfig: JoinRedisNodeConfig = config.Cornucopia.JoinRedisNode
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(JoinRedisNodeSupervisor.props, JoinRedisNodeSupervisor.name)
  }

  object ReshardClusterImpl extends ReshardCluster {
    implicit val reshardClusterConfig: ReshardClusterConfig = config.Cornucopia.ReshardCluster
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    implicit val reshardTable: ReshardTableNew = ReshardTableNewImpl
    val computeReshardTableFactory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(ComputeReshardTable.props, ComputeReshardTable.name)
    val supervisorProps: Props = ReshardClusterSupervisor.props(computeReshardTableFactory)
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ReshardClusterSupervisor.name)
  }

  object ClusterConnectionsImpl extends ClusterConnections {
    implicit val clusterConnectionsConfig: ClusterConnectionsConfig = config.Cornucopia.ClusterConnections
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    implicit val redisHelpers: RedisHelpers = redisHelpersImpl
    val supervisorProps: Props = ClusterConnectionsSupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ClusterConnectionsSupervisor.name)
  }

  object ClusterReadyImpl extends ClusterReady {
    implicit val clusterReadyConfig: ClusterReadyConfig = config.Cornucopia.ClusterReady
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = ClusterReadySupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ClusterReadySupervisor.name)
  }

  object MigrateSlotsImpl extends MigrateSlots {
    implicit val migrateSlotsConfig: MigrateSlotsConfig = config.Cornucopia.MigrateSlots
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val migrateSlotWorkerFactory: (ActorRefFactory, ActorRef) => ActorRef =
      (f: ActorRefFactory, m: ActorRef) => f.actorOf(MigrateSlotWorker.props(m), MigrateSlotWorker.name)
    val supervisorProps: Props = MigrateSlotsSupervisor.props(migrateSlotWorkerFactory)
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, MigrateSlotsSupervisor.name)
  }

  object ReplicatePoorestMasterImpl extends ReplicatePoorestMaster {
    implicit val replicatePoorestMasterConfig: ReplicatePoorestMasterConfig = config.Cornucopia.ReplicatePoorestMaster
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = ReplicatePoorestMasterSupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ReplicatePoorestMasterSupervisor.name)
  }

  object FailoverImpl extends Failover {
    implicit val failoverConfig: FailoverConfig = config.Cornucopia.Failover
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = FailoverSupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, FailoverSupervisor.name)
  }

  object GetSlavesOfMasterImpl extends GetSlavesOfMaster {
    implicit val getSlavesOfMasterConfig: GetSlavesOfMasterConfig = config.Cornucopia.GetSlavesOfMaster
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = GetSlavesOfMasterSupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, GetSlavesOfMasterSupervisor.name)
  }

  object ForgetRedisNodeImpl extends ForgetRedisNode {
    implicit val forgetRedisNodeConfig: ForgetRedisNodeConfig = config.Cornucopia.ForgetRedisNode
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = ForgetRedisNodeSupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ForgetRedisNodeSupervisor.name)
  }

  object ClusterTopologyImpl extends ClusterTopology {
    implicit val clusterTopologyConfig: ClusterTopologyConfig = config.Cornucopia.ClusterTopology
    implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
    val supervisorProps: Props = ClusterTopologySupervisor.props
    val factory: ActorRefFactory => ActorRef =
      (f: ActorRefFactory) => f.actorOf(supervisorProps, ClusterTopologySupervisor.name)
  }

  implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
  val joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef = JoinRedisNodeImpl.factory
  val reshardClusterSupervisorMaker: ActorRefFactory => ActorRef = ReshardClusterImpl.factory
  val clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef = ClusterConnectionsImpl.factory
  val clusterReadySupervisorMaker: ActorRefFactory => ActorRef = ClusterReadyImpl.factory
  val migrateSlotsSupervisorMaker: ActorRefFactory => ActorRef = MigrateSlotsImpl.factory
  val replicatePoorestMasterSupervisorMaker: ActorRefFactory => ActorRef = ReplicatePoorestMasterImpl.factory
  val failoverSupervisorMaker: ActorRefFactory => ActorRef = FailoverImpl.factory
  val getSlavesOfMasterSupervisorMaker: ActorRefFactory => ActorRef = GetSlavesOfMasterImpl.factory
  val forgetRedisNodeSupervisorMaker: ActorRefFactory => ActorRef = ForgetRedisNodeImpl.factory
  val clusterTopologySupervisorMaker: ActorRefFactory => ActorRef = ClusterTopologyImpl.factory

  val overseerProps: Props = Overseer.props(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker,
    clusterConnectionsSupervisorMaker, clusterReadySupervisorMaker, migrateSlotsSupervisorMaker,
    replicatePoorestMasterSupervisorMaker, failoverSupervisorMaker, getSlavesOfMasterSupervisorMaker,
    forgetRedisNodeSupervisorMaker, clusterTopologySupervisorMaker)

  val overseerFactory: ActorRefFactory => ActorRef =
    (f: ActorRefFactory) => f.actorOf(overseerProps, Overseer.name)

  val props: Props = Lifecycle.props(overseerFactory)

  val ref: ActorRef = system.actorOf(props, Lifecycle.name)
}
