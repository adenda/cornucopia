package com.github.kliewkliew.cornucopia.actors

import akka.actor._
import akka.routing.FromConfig
import com.github.kliewkliew.cornucopia.Config
import com.github.kliewkliew.cornucopia.redis.ReshardTable.ReshardTableType
import org.slf4j.LoggerFactory

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

import scala.concurrent.Future

object RedisCommandRouter {
  def props: Props = Props(new RedisCommandRouter)

  case class Migrate(slot: Int, sourceNodeId: String, destinationNodeId: String,
                     fn: (Int, String, String) => Future[Unit])

  case class ReshardCluster(targetNodeId: String, reshardTable: ReshardTableType,
                            migrateSlotFn: (Int, String, String) => Future[Unit])
}

class RedisCommandRouter extends Actor {
  import RedisCommandRouter._

  protected val logger = LoggerFactory.getLogger(this.getClass)

  val migrationRouter: ActorRef =
    context.actorOf(FromConfig.props(Props[Worker]), "migrationRouter")

  def receive = {
    case r: ReshardCluster =>
      logger.info(s"Asked to Reshard cluster")
      reshardCluster(r.targetNodeId, r.reshardTable, r.migrateSlotFn, sender)
    case m: Migrate =>
      logger.info(s"Telling worker to migrate: ${m.slot}")
      migrationRouter.tell(m, sender)
    case Terminated(a) =>
      // TODO: handle termination failure?
      logger.error("Migration router terminated")
  }

  def reshardCluster(targetNodeId: String, reshardTable: ReshardTableType, fn: (Int, String, String) => Future[Unit], ref: ActorRef) = {
    implicit val timeout = Timeout(Config.migrateSlotTimeout seconds)
    implicit val executionContext = Config.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")

    val migrateResults = for {
      (sourceNodeId, slots) <- reshardTable
      slot <- slots
    } yield {
      val msg = Migrate(slot, sourceNodeId, targetNodeId, fn)
      val future = migrationRouter.ask(msg).mapTo[Int]
      future
    }
    val result = Future.fold(migrateResults)(Unit) { (_, slot: Int) =>
      logger.info(s"Got message from worker finished migrating slot $slot")
      Unit
    }
    result map { _ =>
      logger.info(s"Finished migrating all slots")
      ref ! "Done"
    }
  }

}

class Worker extends Actor {
  import RedisCommandRouter._
  implicit val executionContext = Config.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")

  protected val logger = LoggerFactory.getLogger(this.getClass)

  def receive = {
    case Migrate(slot, sourceNodeId, destinationNodeId, fn) =>
      logger.info(s"Worker received migrate message for slot $slot")
      val ref = sender
      fn(slot, sourceNodeId, destinationNodeId) map { res =>
        logger.info(s"Finished migrating for slot $slot")
        ref ! slot
      }
  }

}
