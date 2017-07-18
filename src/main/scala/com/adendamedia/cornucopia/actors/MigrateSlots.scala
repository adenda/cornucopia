package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import akka.pattern.pipe
import akka.actor.Status.{Failure, Success}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ReshardTableNew}
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.ConfigNew.MigrateSlotsConfig
import Overseer.{MigrateSlotsForNewMaster, OverseerCommand, ReshardWithNewMaster}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

object MigrateSlotsSupervisor {
  def props(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotsSupervisor(migrateSlotsWorkerMaker))

  val name = "migrateSlotsSupervisor"
}

/**
  * Actor hierarchy for doing the actual work during a reshard, which is to migrate slots between Redis nodes.
  */
class MigrateSlotsSupervisor(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
                            (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig)
  extends CornucopiaSupervisor {

  val props = MigrateSlotsJobManager.props(migrateSlotsWorkerMaker)
  val migrateSlotsJobManager = context.actorOf(props, MigrateSlotsJobManager.name)

  override def receive: Receive = accepting

  override def accepting: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster =>
      migrateSlotsJobManager forward migrateCommand
      context.become(processing(migrateCommand))
  }

  override def processing(command: OverseerCommand): Receive = {
    case _ =>
  }

}

object MigrateSlotsJobManager {
  def props(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotsJobManager(migrateSlotsWorkerMaker))

  val name = "migrateSlotsJobManager"

  type MigrateSlotJobType = (NodeId, Slot)
  case object GetJob
  case class MigrateSlotJob(sourceNodeId: NodeId, targetNodeId: NodeId, slot: Slot)
  case class JobCompleted(job: MigrateSlotJobType)
}

/**
  * Implements the work-pulling pattern to rate-limit the migrate slot operations
  * @param migrateSlotWorkerMaker Factory to create workers
  * @param clusterOperations
  * @param config
  */
class MigrateSlotsJobManager(migrateSlotWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
                            (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig) extends
  Actor with ActorLogging {

  import Overseer._
  import MigrateSlotsJobManager._

  override def receive: Receive = idle

  private def idle: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster => doMigratingForNewMaster(migrateCommand, sender)
  }

  private def doMigratingForNewMaster(migrateCommand: MigrateSlotsForNewMaster, ref: ActorRef) = {
    val targetNodeId = migrateCommand.redisUriToNodeId(migrateCommand.newMasterUri.toURI.toString)
    val reshardTable = migrateCommand.reshardTable
    val connections = migrateCommand.connections
    val ref = sender

    val pendingSlots = getMigrateJobSet(reshardTable)
    val runningSlots: Set[(NodeId, Slot)] = Set()
    val completedSlots: Set[(NodeId, Slot)] = Set()

    val workers = List.fill(config.numberOfWorkers)(migrateSlotWorkerMaker(context, self)).toSet

    context.become(migratingSlotsForNewMaster(targetNodeId, connections, workers, ref,
                                              pendingSlots, runningSlots, completedSlots))
  }

  /**
    * Convert a reshard table into a set of slot key migrations
    * @param table the reshard table
    * @return 2-tuple containing node ID and slot to migrate
    */
  private def getMigrateJobSet(table: ReshardTableNew.ReshardTableType): Set[(NodeId, Slot)] = {
    for {
      (nodeId, slots) <- table.toSet
      slot <- slots
    } yield (nodeId, slot)
  }

  /**
    * The Job manager is migrating slots until the reshard table is empty
    * @param targetNodeId The new masters node Id
    * @param connections The cluster connections to masters
    * @param workers The set of workers performing slot key migration jobs
    * @param ref The sender
    * @param pendingSlots Migrate slot job not assigned to any worker
    * @param runningSlots Migrate slot job assigned but not yet complete
    * @param completedSlots Migrate slot job completed
    */
  private def migratingSlotsForNewMaster(targetNodeId: NodeId, connections: ClusterOperations.ClusterConnectionsType,
                                         workers: Set[ActorRef], ref: ActorRef,
                                         pendingSlots: Set[(NodeId, Slot)],
                                         runningSlots: Set[(NodeId, Slot)],
                                         completedSlots: Set[(NodeId, Slot)]): Receive = {
    case GetJob =>
      val worker = sender
      getNextSlotToMigrate(pendingSlots) match {
        case Some(migrateSlot) =>
          worker ! MigrateSlotJob(migrateSlot._1, targetNodeId, migrateSlot._2)
          val updatedPendingSlots = pendingSlots - migrateSlot
          val updatedRunningSlots = runningSlots + migrateSlot
          val newState = migratingSlotsForNewMaster(targetNodeId, connections, workers, ref,
                                                    updatedPendingSlots, updatedRunningSlots, completedSlots)
          context.become(newState)
        case None =>
          // TODO: We should be done here. There may be still jobs that are running at this point. We should then kill
          //       the worker by sending a Poison Pill
      }
    case JobCompleted(job: MigrateSlotJobType) =>
      val updatedCompletedSlots = completedSlots + job
      val updatedRunningSlots = runningSlots - job
      // TODO: If running slots is now empty, and pending slots is now empty, that means we are done everything
      val newState = migratingSlotsForNewMaster(targetNodeId, connections, workers, ref,
                                                pendingSlots, updatedRunningSlots, updatedCompletedSlots)
      context.become(newState)
  }

  private def getNextSlotToMigrate(pendingSlots: Set[(NodeId, Slot)]): Option[(NodeId, Slot)] = {
    if (pendingSlots.isEmpty) None
    else pendingSlots.headOption
  }

}

object MigrateSlotWorker {
  def props(jobManager: ActorRef)(implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotWorker(jobManager))

  /**
    * Generate a name with a unique random suffix
    * @return String "migrateSlotWorker-" + randomSuffix
    */
  def name: String = {
    def uuid = java.util.UUID.randomUUID.toString
    "migrateSlotWorker-" + uuid
  }
}

class MigrateSlotWorker(jobManager: ActorRef)
                       (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig) extends
  Actor with ActorLogging {

  import MigrateSlotsJobManager._

  jobManager ! GetJob

  override def receive: Receive = {
    case job: MigrateSlotJob => doJob(job)
  }

  private def doJob(job: MigrateSlotJob) = {
    // TODO
  }
}
