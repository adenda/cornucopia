package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.{Restart, Escalate}
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import akka.pattern.pipe
import akka.actor.Status.{Failure, Success}
import com.adendamedia.cornucopia.redis.{ClusterOperations, ReshardTableNew}
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.ConfigNew.ReshardClusterConfig
import Overseer.{OverseerCommand, ReshardWithNewMaster}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

object ReshardClusterSupervisor {
  def props(computeReshardTableMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: ReshardClusterConfig): Props =
    Props(new ReshardClusterSupervisor(computeReshardTableMaker))

  val name = "reshardClusterSupervisor"

  case object Retry
}

/**
  * Supervises the resharding of the cluster
  *
  * @param computeReshardTableMaker Factory method for creating child actor
  * @param clusterOperations Singleton containing Redis cluster operations
  */
class ReshardClusterSupervisor(computeReshardTableMaker: ActorRefFactory => ActorRef)
                              (implicit clusterOperations: ClusterOperations, config: ReshardClusterConfig)
  extends Actor with ActorLogging {

  import ReshardTableNew.ReshardTableException
  import Overseer._
  import ReshardClusterSupervisor._

  import context.dispatcher

  val getRedisSourceNodesProps = GetRedisSourceNodes.props(computeReshardTableMaker)
  val getRedisSourceNodes = context.actorOf(getRedisSourceNodesProps, GetRedisSourceNodes.name)

  override def supervisorStrategy = OneForOneStrategy(config.maxNrRetries) {
    case _: FailedOverseerCommand => Restart
    case _: ReshardTableException =>
      self ! Retry
      Restart
  }

  override def receive: Receive = {
    case reshard: ReshardWithNewMaster =>
      log.info(s"Resharding with new master ${reshard.uri}")
      getRedisSourceNodes forward reshard
      context.become(resharding(reshard))
  }

  private def resharding(reshard: Reshard): Receive = {
    case Retry =>
      log.info(s"Retrying to reshard cluster")
      getRedisSourceNodes forward reshard
  }

}

object GetRedisSourceNodes {
  def props(computeReshardTableMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations, config: ReshardClusterConfig): Props =
    Props(new GetRedisSourceNodes(computeReshardTableMaker))

  val name = "getRedisSourceNodes"

  case class SourceNodes(nodes: List[RedisClusterNode])
}

class GetRedisSourceNodes(computeReshardTableMaker: ActorRefFactory => ActorRef)
                         (implicit clusterOperations: ClusterOperations, config: ReshardClusterConfig)
  extends Actor with ActorLogging {

  import Overseer._
  import GetRedisSourceNodes._
  import ReshardTableNew.ReshardTableException
  import akka.pattern.pipe

  implicit val ec: ExecutionContext = config.executionContext

  // Escalate this error because it might mean that we need to get the source nodes again
  override def supervisorStrategy = OneForOneStrategy() {
    case e: ReshardTableException => Escalate
  }

  val computeReshardTable = computeReshardTableMaker(context)

  override def receive: Receive = {
    case reshard: ReshardWithNewMaster => getRedisSourceNodes(reshard)
  }

  private def getRedisSourceNodes(reshard: ReshardWithNewMaster) = {
    val uri = reshard.uri
    clusterOperations.getRedisSourceNodes(uri) map { sourceNodes =>
      (reshard.uri, SourceNodes(sourceNodes))
    } pipeTo computeReshardTable
  }

}

object ComputeReshardTable {
  def props(implicit reshardTable: ReshardTableNew, config: ReshardClusterConfig): Props =
    Props(new ComputeReshardTable)

  val name = "computeReshardTable"
}

class ComputeReshardTable(implicit reshardTable: ReshardTableNew, config: ReshardClusterConfig)
  extends Actor with ActorLogging {

  import GetRedisSourceNodes._
  import ReshardTableNew.ReshardTableType

  override def receive: Receive = {
    case (uri: RedisURI, sourceNodes: SourceNodes) =>
      log.info(s"Computing reshard table to add new master ${uri.toURI}")
      computeReshardTable(uri, sourceNodes)
  }

  def computeReshardTable(uri: RedisURI, sourceNodes: SourceNodes) = {
    implicit val expectedTotalNumberSlots: Int = config.expectedTotalNumberSlots
    val table: ReshardTableType = reshardTable.computeReshardTable(sourceNodes.nodes)
  }

}
