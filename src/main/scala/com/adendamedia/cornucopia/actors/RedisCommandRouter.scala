package com.adendamedia.cornucopia.actors

import akka.actor._
import akka.routing.FromConfig
import com.adendamedia.cornucopia.redis.ReshardTable.ReshardTableType
import org.slf4j.LoggerFactory
import akka.pattern.ask
import akka.util.Timeout
import com.adendamedia.cornucopia.Config
import com.lambdaworks.redis.RedisURI
//import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

import scala.concurrent.duration._
import scala.concurrent.Future

object RedisCommandRouter {
  def props: Props = Props(new RedisCommandRouter)

  case class Migrate(slot: Int, sourceNodeId: String, destinationNodeId: String,
                     fn: (Int, String, String) => Future[Unit])

  case class MigratePrime(slot: Int, sourceNodeId: String, targetNodeId: String,
                          fn: (Int, String, String, RedisURI) => Future[Unit], idToURI: Map[String, RedisURI])

  /**
    * Used when resharding a cluster with a new master
    *
    * @param targetNodeId The node Id of the new master
    * @param reshardTable The reshard table to perform the resharding
    * @param migrateSlotFn The partially applied function to migrate slot data to the new master
    */
  case class ReshardCluster(targetNodeId: String, reshardTable: ReshardTableType,
                            migrateSlotFn: (Int, String, String) => Future[Unit])

  /**
    * Used when resharding a cluster when removing a master node
    *
    * @param sourceNodeId The retired master that is relinquishing all of its slot nodes
    * @param reshardTable The reshard table to perform the resharding
    * @param migrateSlotFn The partially applied function to migrate the slot data to the remaining masters
    * @param idToURI Mapping from Redis node Id to RedisURI of all live masters
    */
  case class ReshardClusterPrime(sourceNodeId: String, reshardTable: ReshardTableType,
                                 migrateSlotFn: (Int, String, String, RedisURI) => Future[Unit],
                                 idToURI: Map[String, RedisURI])
}

class RedisCommandRouter extends Actor {
  import RedisCommandRouter._

  protected val logger = LoggerFactory.getLogger(this.getClass)

  val migrationRouter: ActorRef =
    context.actorOf(FromConfig.props(Props[Worker]), "migrationRouter")

  def receive = {
    case r: ReshardCluster =>
      logger.info(s"Asked to Reshard cluster with new master")
      reshardCluster(r.targetNodeId, r.reshardTable, r.migrateSlotFn, sender)
    case r: ReshardClusterPrime =>
      logger.info(s"Asked to Reshard cluster by removing an existing master")
      reshardClusterPrime(r.sourceNodeId, r.reshardTable, r.migrateSlotFn, r.idToURI, sender)
    case m: Migrate =>
      logger.info(s"Telling worker to migrate: ${m.slot}")
      migrationRouter.tell(m, sender)
    case m: MigratePrime =>
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

  def reshardClusterPrime(sourceNodeId: String, reshardTable: ReshardTableType,
                          fn: (Int, String, String, RedisURI) => Future[Unit], idToURI: Map[String, RedisURI],
                          ref: ActorRef) = {
    implicit val timeout = Timeout(Config.migrateSlotTimeout seconds)
    implicit val executionContext = Config.actorSystem.dispatchers.lookup("akka.actor.resharding-dispatcher")

    val migrateResults = for {
      (targetNodeId, slots) <- reshardTable
      slot <- slots
    } yield {
      val msg = MigratePrime(slot, sourceNodeId, targetNodeId, fn, idToURI)
      val future = migrationRouter.ask(msg).mapTo[Int]
      future
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
      fn(slot, sourceNodeId, destinationNodeId) map { _ =>
        logger.info(s"Finished migrating for slot $slot")
        ref ! slot
      }
    case MigratePrime(slot, sourceNodeId, destinationNodeId, fn, idToURI) =>
      logger.info(s"Worker received migrate message for slot $slot")
      val ref = sender
      fn(slot, sourceNodeId, destinationNodeId, idToURI(sourceNodeId)) map { _ =>
        logger.info(s"Finished migrating for slot $slot")
        ref ! slot
      }
  }

}
