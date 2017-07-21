package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.pattern.pipe
import akka.actor.Status.{Failure, Success}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ReshardTableNew}
import com.adendamedia.cornucopia.redis.ReshardTableNew._
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.ConfigNew.MigrateSlotsConfig
import Overseer.{JobCompleted, MigrateSlotsForNewMaster, OverseerCommand, ReshardWithNewMaster}
import com.adendamedia.cornucopia.redis.ClusterOperations.{MigrateSlotKeysMovedException, SetSlotAssignmentException}

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

  import Overseer._

  val props = MigrateSlotsJobManager.props(migrateSlotsWorkerMaker)
  val migrateSlotsJobManager = context.actorOf(props, MigrateSlotsJobManager.name)

  override def receive: Receive = accepting

  override def accepting: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster =>
      migrateSlotsJobManager forward migrateCommand
      context.become(processing(migrateCommand))
  }

  override def processing(command: OverseerCommand): Receive = {
    case Reset =>
      log.debug("Reset migrate slot supervisor")
      context.become(accepting)
  }

}

object MigrateSlotsJobManager {
  def props(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotsJobManager(migrateSlotsWorkerMaker))

  val name = "migrateSlotsJobManager"

  type MigrateSlotJobType = (NodeId, Slot)
  case object GetJob
  case class MigrateSlotJob(sourceNodeId: NodeId, targetNodeId: NodeId, slot: Slot,
                            connections: ClusterOperations.ClusterConnectionsType,
                            redisURI: Option[RedisURI] = None) extends OverseerCommand
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

  override def supervisorStrategy = OneForOneStrategy() {
    case e: MigrateSlotsException =>
      // reshedule the failed slot migration
      val msg = e.command
      Resume
  }

  override def receive: Receive = idle

  private def idle: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster => doMigratingForNewMaster(migrateCommand, sender)
  }

  private def doMigratingForNewMaster(migrateCommand: MigrateSlotsForNewMaster, ref: ActorRef) = {
    val targetNodeId = migrateCommand.redisUriToNodeId(migrateCommand.newMasterUri.toString)
    val reshardTable = migrateCommand.reshardTable
    val connections = migrateCommand.connections
    val ref = sender

    val pendingSlots = getMigrateJobSet(reshardTable)
    val runningSlots: Set[(NodeId, Slot)] = Set()
    val completedSlots: Set[(NodeId, Slot)] = Set()

    val workers = List.fill(config.numberOfWorkers)(migrateSlotWorkerMaker(context, self)).toSet

    context.become(migratingSlotsForNewMaster(targetNodeId, connections, migrateCommand, workers, ref,
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
                                         cmd: MigrateSlotsForNewMaster, workers: Set[ActorRef], ref: ActorRef,
                                         pendingSlots: Set[(NodeId, Slot)],
                                         runningSlots: Set[(NodeId, Slot)],
                                         completedSlots: Set[(NodeId, Slot)]): Receive = {
    case GetJob =>
      val worker = sender
      getNextSlotToMigrate(pendingSlots) match {
        case Some(migrateSlot) =>
          worker ! MigrateSlotJob(migrateSlot._1, targetNodeId, migrateSlot._2, connections, Some(cmd.newMasterUri))
          val updatedPendingSlots = pendingSlots - migrateSlot
          val updatedRunningSlots = runningSlots + migrateSlot
          val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
                                                    updatedPendingSlots, updatedRunningSlots, completedSlots)
          context.become(newState)
        case None =>
          // Keep workers around till we're sure all the slots
          // have been migrated, which happens when pendingSlots AND runningSlots is empty
          // So, in here check for if those two sets are empty, which means this worker processed the last successful
          // message. Then if that's the case, send a PoisonPill to all workers, and change behaviour/state to idle
          // again.
          if (pendingSlots.isEmpty && runningSlots.isEmpty) finishJob(cmd, ref, workers)
      }
    case JobCompleted(job: MigrateSlotJob) =>
      log.info(s"Successfully migrated slot ${job.slot} from ${job.sourceNodeId} to $targetNodeId")
      val migratedSlot: MigrateSlotJobType = (job.sourceNodeId, job.slot)
      val updatedCompletedSlots = completedSlots + migratedSlot
      val updatedRunningSlots = runningSlots - migratedSlot
      val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
                                                pendingSlots, updatedRunningSlots, updatedCompletedSlots)
      context.become(newState)
  }

  private def getNextSlotToMigrate(pendingSlots: Set[(NodeId, Slot)]): Option[(NodeId, Slot)] = {
    if (pendingSlots.isEmpty) None
    else pendingSlots.headOption
  }

  private def finishJob(cmd: MigrateSlotsForNewMaster, ref: ActorRef, workers: Set[ActorRef]) = {
    workers.foreach(_ ! PoisonPill)
    ref ! JobCompleted(cmd)
    context.become(idle)
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

  val setSlotAssignmentWorker: ActorRef =
    context.actorOf(SetSlotAssignmentWorker.props(self), SetSlotAssignmentWorker.name)

  jobManager ! GetJob

  override def supervisorStrategy = OneForOneStrategy() {
    case e: MigrateSlotsException =>
      e.reason match {
        case Some(t: SetSlotAssignmentException) => Restart
        case _ =>
          log.error(s"Fatal error trying to migrate slot")
          Escalate
      }
  }

  override def receive: Receive = {
    case job: MigrateSlotJob => setSlotAssignmentWorker ! job
    case complete: JobCompleted =>
      log.debug(s"Job complete")
      jobManager ! complete
      jobManager ! GetJob
  }

}

object SetSlotAssignmentWorker {
  def props(topLevelWorker: ActorRef)(implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new SetSlotAssignmentWorker(topLevelWorker))

  val name = "setSlotAssignmentWorker"
}

class SetSlotAssignmentWorker(topLevelWorker: ActorRef)
                             (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig)
  extends Actor with ActorLogging {

  import Overseer._
  import MigrateSlotsJobManager._
  import ClusterOperations._

  val migrateSlotKeysWorker: ActorRef =
    context.actorOf(MigrateSlotKeysWorker.props(topLevelWorker), MigrateSlotKeysWorker.name)

  override def supervisorStrategy = OneForOneStrategy() {
    case e: MigrateSlotsException =>
      e.reason match {
        case Some(t: MigrateSlotKeysBusyKeyException) => Restart
        case Some(t: MigrateSlotKeysClusterDownException) => Escalate
        case Some(t: MigrateSlotKeysMovedException) =>
          log.debug(s"Slot keys moved, silently ignoring: ${t.reason}")
          topLevelWorker ! e.command
          Resume
        case _ => Escalate
      }
  }

  override def receive: Receive = {
    case job: MigrateSlotJob => doJob(job)
    case e: KillChild => throw MigrateSlotsException(e.command, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.debug(s"Setting slot assignment before migrating slot ${job.slot} from ${job.sourceNodeId} to ${job.targetNodeId}")

    implicit val executionContext = config.executionContext

    clusterOperations.setSlotAssignment(job.slot, job.sourceNodeId, job.targetNodeId, job.connections) map { _ =>
      job
    } recover {
      case e: SetSlotAssignmentException => self ! KillChild(command = job, reason = Some(e))
    } pipeTo migrateSlotKeysWorker
  }

}

object MigrateSlotKeysWorker {
  def props(topLevelWorker: ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotKeysWorker(topLevelWorker))

  val name = "migrateSlotKeysWorker"
}

class MigrateSlotKeysWorker(topLevelWorker: ActorRef)
                           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig)
  extends Actor with ActorLogging {

  import Overseer._
  import MigrateSlotsJobManager._

  val notifySlotAssignmentWorker: ActorRef =
    context.actorOf(NotifySlotAssignmentWorker.props(topLevelWorker), NotifySlotAssignmentWorker.name)

  override def supervisorStrategy = OneForOneStrategy() {
    case e: MigrateSlotsException => Escalate
  }

  override def receive: Receive = {
    case job: MigrateSlotJob => doJob(job)
    case e: KillChild => throw MigrateSlotsException(e.command, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.info(s"Migrating slot keys for slot ${job.slot} from ${job.sourceNodeId} to ${job.targetNodeId}")

    implicit val executionContext = config.executionContext
    clusterOperations.migrateSlotKeys(job.slot, job.redisURI.get, job.sourceNodeId, job.targetNodeId, job.connections) map { _ =>
      job
    } recover {
      case e: MigrateSlotKeysMovedException =>
        log.debug(s"Slot keys moved, moving along: ", e.reason)
        log.info(s"")
        notifySlotAssignmentWorker ! job
      case e =>
        self ! KillChild(command = job, reason = Some(e))
    } pipeTo notifySlotAssignmentWorker
  }

}

object NotifySlotAssignmentWorker {
  def props(topLevelWorker: ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new NotifySlotAssignmentWorker(topLevelWorker))

  val name = "notifySlotAssignmentWorker"
}

class NotifySlotAssignmentWorker(topLevelWorker: ActorRef)
                                (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig)
  extends Actor with ActorLogging {

  import Overseer._
  import MigrateSlotsJobManager._

  override def receive: Receive = {
    case job: MigrateSlotJob => doJob(job)
    case e: KillChild => throw MigrateSlotsException(e.command, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.info(s"Notifying slot assignment for slot ${job.slot} which now lives on node ${job.targetNodeId}")

    implicit val executionContext = config.executionContext
    clusterOperations.notifySlotAssignment(job.slot, job.targetNodeId, job.connections) map { _ =>
      JobCompleted(job)
    } recover {
      case e =>
        self ! KillChild(command = job, reason = Some(e))
    } pipeTo topLevelWorker
  }
}
