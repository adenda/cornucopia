package com.adendamedia.cornucopia

import akka.actor.{ActorRef, ActorRefFactory, Props, ActorSystem}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ClusterOperationsImpl}
import com.adendamedia.cornucopia.actors._
import com.adendamedia.cornucopia.redis._

object Cornucopia {
  import ConfigNew._

  trait JoinRedisNode {
    implicit val maxNrRetries: Int
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

}

class Cornucopia {
  import Cornucopia._
  import ConfigNew._

  private val config = new ConfigNew

  private val system: ActorSystem = config.actorSystem

  private val clusterOperationsImpl: ClusterOperations = ClusterOperationsImpl

  object JoinRedisNodeImpl extends JoinRedisNode {
    implicit val maxNrRetries: Int = config.Cornucopia.JoinRedisNode.maxNrRetries
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

  implicit val clusterOperations: ClusterOperations = clusterOperationsImpl
  val joinRedisNodeSupervisorMaker: ActorRefFactory => ActorRef = JoinRedisNodeImpl.factory
  val reshardClusterSupervisorMaker: ActorRefFactory => ActorRef = ReshardClusterImpl.factory
  val clusterConnectionsSupervisorMaker: ActorRefFactory => ActorRef = ClusterConnectionsImpl.factory
  val clusterReadySupervisorMaker: ActorRefFactory => ActorRef = ClusterReadyImpl.factory
  val migrateSlotsSupervisorMaker: ActorRefFactory => ActorRef = MigrateSlotsImpl.factory
  val props: Props = Overseer.props(joinRedisNodeSupervisorMaker, reshardClusterSupervisorMaker,
    clusterConnectionsSupervisorMaker, clusterReadySupervisorMaker, migrateSlotsSupervisorMaker)
  val ref: ActorRef = system.actorOf(props, Overseer.name)

}
