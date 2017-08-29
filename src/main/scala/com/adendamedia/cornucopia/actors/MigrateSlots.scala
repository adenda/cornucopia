package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume, Stop}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, AllForOneStrategy, PoisonPill, Props, Terminated}
import akka.pattern.pipe
import com.adendamedia.cornucopia.redis.{ClusterOperations, ReshardTable}
import com.adendamedia.cornucopia.redis.ReshardTable._
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.Config.MigrateSlotsConfig
import Overseer.{JobCompleted, MigrateSlotsCommand, OverseerCommand}
import com.adendamedia.cornucopia.redis.ClusterOperations.{MigrateSlotKeysMovedException, SetSlotAssignmentException}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.lambdaworks.redis.RedisURI

object MigrateSlotsSupervisor {
  def props(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotsSupervisor(migrateSlotsWorkerMaker))

  val name = "migrateSlotsSupervisor"

  case object Kill
}

/**
  * Actor hierarchy for doing the actual work during a reshard, which is to migrate slots between Redis nodes.
  */
class MigrateSlotsSupervisor[C <: MigrateSlotsCommand](migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
                            (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig)
  extends CornucopiaSupervisor[MigrateSlotsCommand] {

  import Overseer._
  import MigrateSlotsSupervisor._

  private val props = MigrateSlotsJobManager.props(migrateSlotsWorkerMaker)
  private val migrateSlotsJobManager = context.actorOf(props, MigrateSlotsJobManager.name)

  override def supervisorStrategy = AllForOneStrategy() {
    case e: MigrateSlotsJobManagerException =>
      self ! Kill
      Stop
  }

  override def receive: Receive = accepting

  override def accepting: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster =>
      log.info(s"Migrating slots for adding new master ${migrateCommand.newMasterUri}")
      migrateSlotsJobManager ! migrateCommand
      context.become(processing(migrateCommand, sender))
    case migrateCommand: MigrateSlotsWithoutRetiredMaster =>
      log.info(s"Migrating slots without retired master ${migrateCommand.retiredMasterUri}")
      migrateSlotsJobManager ! migrateCommand
      context.become(processing(migrateCommand, sender))
  }

  override def processing[D <: MigrateSlotsCommand](command: D, ref: ActorRef): Receive = {
    case Reset =>
      log.debug("Reset migrate slot supervisor")
      context.become(accepting)
    case msg: JobCompleted =>
      log.debug("Job completed")
      ref forward msg
      context.become(accepting)
    case Kill =>
      throw FailedOverseerCommand("Goodbye cruel world", command)
  }

}

object MigrateSlotsJobManager {
  def props(migrateSlotsWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig): Props =
    Props(new MigrateSlotsJobManager(migrateSlotsWorkerMaker))

  val name = "migrateSlotsJobManager"

  type MigrateSlotJobType = (NodeId, Slot)
  case object GetJob
  case class Kill(job: Overseer.MigrateSlotJob, reason: Option[Throwable] = None)
  case class RescheduleJob(job: Overseer.MigrateSlotJob)
  case class JobHasFailed(job: Overseer.MigrateSlotJob)
}

/**
  * Implements the work-pulling pattern to rate-limit the migrate slot operations
  * @param migrateSlotWorkerMaker Factory to create workers
  * @param clusterOperations Operations on redis cluster
  * @param config configuration object for slot migration
  */
class MigrateSlotsJobManager(migrateSlotWorkerMaker: (ActorRefFactory, ActorRef) => ActorRef)
                            (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig) extends
  Actor with ActorLogging {

  import Overseer._
  import MigrateSlotsJobManager._

  override def supervisorStrategy = AllForOneStrategy() {
    case e: MigrateSlotsException =>
      log.error(s"Migrate slot worker failed: {}", e)
      // reschedule the failed slot migration
      val msg = e.job
      self ! RescheduleJob(msg)
      Restart
    case e: FailedSlotMigrationJobException =>
      self ! JobHasFailed(e.job)
      Restart
  }

  override def receive: Receive = idle

  private def idle: Receive = {
    case migrateCommand: MigrateSlotsForNewMaster => doMigratingForNewMaster(migrateCommand, sender)
    case migrateCommand: MigrateSlotsWithoutRetiredMaster => doMigratingForRetiredMaster(migrateCommand, sender)
  }

  private def doMigratingForRetiredMaster(migrateCommand: MigrateSlotsWithoutRetiredMaster, ref: ActorRef) = {
    val targetNodeId = migrateCommand.redisUriToNodeId(migrateCommand.retiredMasterUri)
    val reshardTable = migrateCommand.reshardTable
    val connections = migrateCommand.connections
    val nodeIdToRedisUri = migrateCommand.nodeIdToRedisUri
    val ref = sender

    val pendingSlots = getMigrateJobSet(reshardTable)
    val runningSlots: Set[(NodeId, Slot)] = Set()
    val completedSlots: Set[(NodeId, Slot)] = Set()
    val failedSlots: Set[(NodeId, Slot)] = Set()

    val numWorkers = config.numberOfWorkers
    log.info(s"Starting $numWorkers workers for running slot migration job")

    val workers = List.fill(numWorkers)(migrateSlotWorkerMaker(context, self)).toSet

    context.become(migratingSlotsWithoutRetiredMaster(targetNodeId, connections, nodeIdToRedisUri, migrateCommand,
      workers, ref, pendingSlots, runningSlots, completedSlots, failedSlots))
  }

  private def doMigratingForNewMaster(migrateCommand: MigrateSlotsForNewMaster, ref: ActorRef) = {
    val targetNodeId = migrateCommand.redisUriToNodeId(migrateCommand.newMasterUri)
    val reshardTable = migrateCommand.reshardTable
    val connections = migrateCommand.connections
    val ref = sender

    val pendingSlots = getMigrateJobSet(reshardTable)
    val runningSlots: Set[(NodeId, Slot)] = Set()
    val completedSlots: Set[(NodeId, Slot)] = Set()
    val failedSlots: Set[(NodeId, Slot)] = Set()

    val workers: Set[ActorRef] = List.fill(config.numberOfWorkers)(migrateSlotWorkerMaker(context, self)).toSet

    context.become(migratingSlotsForNewMaster(targetNodeId, connections, migrateCommand, workers, ref,
                                              pendingSlots, runningSlots, completedSlots, failedSlots))
  }

  /**
    * Convert a reshard table into a set of slot key migrations
    * @param table the reshard table
    * @return 2-tuple containing node ID and slot to migrate
    */
  private def getMigrateJobSet(table: ReshardTable.ReshardTableType): Set[(NodeId, Slot)] = {
    for {
      (nodeId, slots) <- table.toSet
      slot <- slots
    } yield (nodeId, slot)
  }

  /**
    * The job manager is migrating slots until the reshard table has been processed completely
    * @param retiredNodeId The retired master node Id
    * @param connections The cluster connections to master nodes
    * @param cmd The Overseer command that triggered the migrate slots stage
    * @param workers A set of workers for performing the migration
    * @param ref The actor ref of the supervisor of the job manager
    * @param pendingSlots Migrate slot job not assigned to any worker
    * @param runningSlots Migrate slot job assigned but not yet complete
    * @param completedSlots Migrate slot job completed
    */
  private def migratingSlotsWithoutRetiredMaster(retiredNodeId: NodeId,
                                                 connections: ClusterOperations.ClusterConnectionsType,
                                                 nodeIdToRedisUri: ClusterOperations.NodeIdToRedisUri,
                                                 cmd: MigrateSlotsWithoutRetiredMaster, workers: Set[ActorRef],
                                                 ref: ActorRef,
                                                 pendingSlots: Set[(NodeId, Slot)],
                                                 runningSlots: Set[(NodeId, Slot)],
                                                 completedSlots: Set[(NodeId, Slot)],
                                                 failedSlots: Set[(NodeId, Slot)]): Receive = {
    case GetJob =>
      val worker = sender
      getNextSlotToMigrate(pendingSlots) match {
        case Some(migrateSlot) =>
          // The redis URI needs to be the target URI
          val sourceNodeId = retiredNodeId
          val targetNodeId = migrateSlot._1
          val targetRedisUri = nodeIdToRedisUri.get(targetNodeId)
          worker ! MigrateSlotJob(sourceNodeId, targetNodeId, migrateSlot._2, connections, targetRedisUri)
          val updatedPendingSlots = pendingSlots - migrateSlot
          val updatedRunningSlots = runningSlots + migrateSlot
          val newState = migratingSlotsWithoutRetiredMaster(retiredNodeId, connections, nodeIdToRedisUri, cmd, workers,
            ref, updatedPendingSlots, updatedRunningSlots, completedSlots, failedSlots)
          context.unbecome()
          context.become(newState)
        case None =>
          // Keep workers around till we're sure all the slots
          // have been migrated, which happens when pendingSlots AND runningSlots is empty
          // So, in here check for if those two sets are empty, which means this worker processed the last successful
          // message. Then if that's the case, send a PoisonPill to all workers, and change behaviour/state to idle
          // again.
          log.info(s"No more jobs left")
          if (pendingSlots.isEmpty && runningSlots.isEmpty) finishJob(cmd, ref, workers, failedSlots)
      }
    case JobCompleted(job: MigrateSlotJob) =>
      log.debug(s"Successfully migrated slot ${job.slot} from $retiredNodeId to ${job.targetNodeId}")
      val migratedSlot: MigrateSlotJobType = (job.targetNodeId, job.slot)
      val updatedCompletedSlots = completedSlots + migratedSlot
      val updatedRunningSlots = runningSlots - migratedSlot
      val newState = migratingSlotsWithoutRetiredMaster(retiredNodeId, connections, nodeIdToRedisUri, cmd, workers, ref,
        pendingSlots, updatedRunningSlots, updatedCompletedSlots, failedSlots)
      context.unbecome()
      context.become(newState)
    case RescheduleJob(job: MigrateSlotJob) =>
      val targetNodeId = job.targetNodeId
      val slot = job.slot
      val migrateSlot = (targetNodeId, slot)
      val updatedPendingSlots = pendingSlots + migrateSlot
      val updatedRunningSlots = runningSlots - migrateSlot
      val newState = migratingSlotsWithoutRetiredMaster(retiredNodeId, connections, nodeIdToRedisUri, cmd, workers,
        ref, updatedPendingSlots, updatedRunningSlots, completedSlots, failedSlots)
      context.unbecome()
      context.become(newState)
    case JobHasFailed(job: MigrateSlotJob) =>
      log.error(s"Migrate slot job has failed: $job")
      if (failedSlots.size + 1 >= config.failureThreshold) {
        throw FailedOverseerCommand(s"Migrate slots job has failed to process command $cmd", cmd)
      }
      val slot = job.slot
      val targetNodeId = job.targetNodeId
      val migrateSlot = (targetNodeId, slot)
      val updatedFailedSlots = failedSlots + migrateSlot
      val updatedRunningSlots = runningSlots - migrateSlot
      val newState = migratingSlotsWithoutRetiredMaster(retiredNodeId, connections, nodeIdToRedisUri, cmd, workers,
        ref, pendingSlots, updatedRunningSlots, completedSlots, updatedFailedSlots)
      context.unbecome()
      context.become(newState)
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
                                         completedSlots: Set[(NodeId, Slot)],
                                         failedSlots: Set[(NodeId, Slot)]): Receive = {
    case GetJob =>
      val worker = sender
      getNextSlotToMigrate(pendingSlots) match {
        case Some(migrateSlot) =>
          worker ! MigrateSlotJob(migrateSlot._1, targetNodeId, migrateSlot._2, connections, Some(cmd.newMasterUri))
          val updatedPendingSlots = pendingSlots - migrateSlot
          val updatedRunningSlots = runningSlots + migrateSlot
          val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
                                                    updatedPendingSlots, updatedRunningSlots, completedSlots, failedSlots)
          context.unbecome()
          context.become(newState)
        case None =>
          // Keep workers around till we're sure all the slots
          // have been migrated, which happens when pendingSlots AND runningSlots is empty
          // So, in here check for if those two sets are empty, which means this worker processed the last successful
          // message. Then if that's the case, send a PoisonPill to all workers, and change behaviour/state to idle
          // again.
          log.info(s"No more jobs left")
          if (pendingSlots.isEmpty && runningSlots.isEmpty) finishJob(cmd, ref, workers, failedSlots)
      }
    case JobCompleted(job: MigrateSlotJob) =>
      log.debug(s"Successfully migrated slot ${job.slot} from ${job.sourceNodeId} to $targetNodeId")
      val migratedSlot: MigrateSlotJobType = (job.sourceNodeId, job.slot)
      val updatedCompletedSlots = completedSlots + migratedSlot
      val updatedRunningSlots = runningSlots - migratedSlot
      context.unbecome()
      val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
                                                pendingSlots, updatedRunningSlots, updatedCompletedSlots, failedSlots)
      context.become(newState)
    case RescheduleJob(job: MigrateSlotJob) =>
      val sourceNodeId = job.sourceNodeId
      val slot = job.slot
      val migrateSlot = (sourceNodeId, slot)
      val updatedPendingSlots = pendingSlots + migrateSlot
      val updatedRunningSlots = runningSlots - migrateSlot
      val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
        updatedPendingSlots, updatedRunningSlots, completedSlots, failedSlots)
      context.unbecome()
      context.become(newState)
    case JobHasFailed(job: MigrateSlotJob) =>
      log.error(s"Migrate slot job has failed: $job")
      if (failedSlots.size + 1 >= config.failureThreshold) {
        throw FailedOverseerCommand(s"Migrate slots job has failed to process command $cmd", cmd)
      }
      val slot = job.slot
      val sourceNodeId = job.sourceNodeId
      val migrateSlot = (sourceNodeId, slot)
      val updatedFailedSlots = failedSlots + migrateSlot
      val updatedRunningSlots = runningSlots - migrateSlot
      val newState = migratingSlotsForNewMaster(targetNodeId, connections, cmd, workers, ref,
        pendingSlots, updatedRunningSlots, completedSlots, updatedFailedSlots)
      context.unbecome()
      context.become(newState)
  }

  private def getNextSlotToMigrate(pendingSlots: Set[(NodeId, Slot)]): Option[(NodeId, Slot)] = {
    if (pendingSlots.isEmpty) None
    else pendingSlots.headOption
  }

  private def finishJob(cmd: OverseerCommand, ref: ActorRef, workers: Set[ActorRef], failedSlots: Set[(NodeId, Slot)]) = {
    log.info(s"All jobs have completed, sending poison pill to all workers")
    logFailedSlots(failedSlots)
    workers.foreach(_ ! PoisonPill)
    ref ! JobCompleted(cmd)
    context.unbecome() // idle
  }

  private def logFailedSlots(failedSlots: Set[(NodeId, Slot)]) = {
    log.warning(s"The following slot migration jobs failed: ${failedSlots.mkString(", ")}")
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

  case object Retry
  case class RegisterPeonWorker(ref: ActorRef)
}

class MigrateSlotWorker(jobManager: ActorRef)
                       (implicit clusterOperations: ClusterOperations, config: MigrateSlotsConfig) extends
  Actor with ActorLogging {

  import MigrateSlotsJobManager._
  import MigrateSlotWorker._
  import Overseer.MigrateSlotJob

  var watchList: List[ActorRef] = List()

  val setSlotAssignmentWorker: ActorRef =
    context.actorOf(SetSlotAssignmentWorker.props(self), SetSlotAssignmentWorker.name)

  context.watch(setSlotAssignmentWorker)

  override def preStart(): Unit = {
    log.info(s"Worker at path ${self.path} is alive: Give me a job!")
    jobManager ! GetJob
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"Worker was restarted at path ${self.path}: Give me another job!")
    jobManager ! GetJob
  }

  override def supervisorStrategy = AllForOneStrategy(config.maxNrRetries) {
    case e: MigrateSlotsException =>
      e.reason match {
        case Some(t: SetSlotAssignmentException) =>
          implicit val executionContext: ExecutionContext = config.executionContext
          context.system.scheduler.scheduleOnce(config.setSlotAssignmentRetryBackoff.seconds)(self ! Retry)
          Restart
        case _ =>
          log.error(s"Fatal error trying to migrate slot {}", e.reason.getOrElse(new Exception("Unknown error")))
          throw e
      }
  }

  override def receive: Receive = {
    case job: MigrateSlotJob =>
      setSlotAssignmentWorker ! job
      context.become(migratingSlot(job, List()))
  }

  def migratingSlot(job: MigrateSlotJob, watchList: List[ActorRef]): Receive = {
    case complete: JobCompleted =>
      log.debug(s"Job complete")
      implicit val executionContext: ExecutionContext = config.executionContext
      jobManager ! complete
      jobManager ! GetJob
      context.unbecome()
    case Retry =>
      log.info(s"Retrying to set slot assignment for slot ${job.slot}")
      setSlotAssignmentWorker ! job
    case Terminated(ref: ActorRef) =>
      // indicates that this slot job is failed
      // TODO: choose exception message based on what actor terminated
      watchList.filter(_ != ref).foreach(context.stop)
      watchList.foreach(context.unwatch)
      throw FailedSlotMigrationJobException(s"Migrate slot job failed while trying to set slot assignment for job $job", job)
    case RegisterPeonWorker(ref: ActorRef) =>
      context.watch(ref)
      context.unbecome()
      context.become(migratingSlot(job, List(ref) ++ watchList))
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
  import MigrateSlotWorker._

  val migrateSlotKeysWorker: ActorRef =
    context.actorOf(MigrateSlotKeysWorker.props(topLevelWorker), MigrateSlotKeysWorker.name)

  override def supervisorStrategy = AllForOneStrategy(config.maxNrRetries) {
    case e: MigrateSlotsException =>
      implicit val executionContext: ExecutionContext = config.executionContext
      context.system.scheduler.scheduleOnce(config.setSlotAssignmentRetryBackoff.seconds)(migrateSlotKeysWorker ! e.job)
      Restart
  }

  override def receive: Receive  = waiting

  private def waiting: Receive = {
    case job: MigrateSlotJob =>
      topLevelWorker ! RegisterPeonWorker(migrateSlotKeysWorker)
      context.become(working)
      doJob(job)
  }

  private def working: Receive = {
    case job: MigrateSlotJob => doJob(job)
    case e: Kill =>
      log.error(s"Error migrating slot {}", e.reason.getOrElse(new Exception("Unknown error")))
      throw MigrateSlotsException(s"Command failed: ${e.job}", e.job, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.debug(s"Setting slot assignment before migrating slot ${job.slot} from ${job.sourceNodeId} to ${job.targetNodeId}")

    implicit val executionContext: ExecutionContext = config.executionContext

    clusterOperations.setSlotAssignment(job.slot, job.sourceNodeId, job.targetNodeId, job.connections) map { _ =>
      job
    } recover {
      case e: SetSlotAssignmentException =>
        self ! Kill(job = job, reason = Some(e))
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
    case e: MigrateSlotsException =>
      val job = e.job
      val reason = e.reason.getOrElse(new Exception("unknown error"))
      log.error(s"There was an error notifying slot assignment for job $job: {}", reason)
      Resume
  }

  override def receive: Receive = {
    case job: MigrateSlotJob => doJob(job)
    case e: Kill => throw MigrateSlotsException(s"Command failed: ${e.job}", e.job, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.debug(s"Migrating slot keys for slot ${job.slot} from ${job.sourceNodeId} to ${job.targetNodeId}")

    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.migrateSlotKeys(job.slot, job.redisURI.get, job.sourceNodeId, job.targetNodeId, job.connections) map { _ =>
      job
    } recover {
      case e: MigrateSlotKeysMovedException =>
        log.debug(s"Slot keys moved, moving along: {}", e.reason)
        notifySlotAssignmentWorker ! job
      case e =>
        self ! Kill(job = job, reason = Some(e))
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
    case e: Kill => throw MigrateSlotsException(s"Command failed: ${e.job}", e.job, e.reason)
  }

  private def doJob(job: MigrateSlotJob) = {
    log.debug(s"Notifying slot assignment for slot ${job.slot} which now lives on node ${job.targetNodeId}")

    implicit val executionContext: ExecutionContext = config.executionContext
    clusterOperations.notifySlotAssignment(job.slot, job.targetNodeId, job.connections) map { _ =>
      JobCompleted(job)
    } recover {
      case e =>
        self ! Kill(job = job, reason = Some(e))
    } pipeTo topLevelWorker
  }
}
