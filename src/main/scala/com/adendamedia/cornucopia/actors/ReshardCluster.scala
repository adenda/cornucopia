package com.adendamedia.cornucopia.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, OneForOneStrategy, Props, Terminated}
import akka.pattern.pipe
import akka.actor.Status.{Failure, Success}
import com.adendamedia.cornucopia.redis.ClusterOperations
import com.adendamedia.cornucopia.CornucopiaException._
import com.adendamedia.cornucopia.redis.ReshardTableNew
import Overseer.{OverseerCommand, ReshardWithNewMaster}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import com.lambdaworks.redis.RedisURI
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

object ReshardClusterSupervisor {
  def props(computeReshardTableMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations): Props = Props(new ReshardClusterSupervisor(computeReshardTableMaker))

  val name = "reshardClusterSupervisor"
}

class ReshardClusterSupervisor(computeReshardTableMaker: ActorRefFactory => ActorRef)
                              (implicit clusterOperations: ClusterOperations)
  extends Actor with ActorLogging {

  import Overseer._

  import context.dispatcher

  val getRedisSourceNodesProps = GetRedisSourceNodes.props(computeReshardTableMaker)
  val getRedisSourceNodes = context.actorOf(getRedisSourceNodesProps, GetRedisSourceNodes.name)

  override def supervisorStrategy = OneForOneStrategy() {
    case _: FailedOverseerCommand => Restart
  }

  override def receive: Receive = {
    case reshard: ReshardWithNewMaster => getRedisSourceNodes forward reshard
  }

}

object GetRedisSourceNodes {
  def props(computeReshardTableMaker: ActorRefFactory => ActorRef)
           (implicit clusterOperations: ClusterOperations, executionContext: ExecutionContext): Props =
    Props(new GetRedisSourceNodes(computeReshardTableMaker))

  val name = "getRedisSourceNodes"
}

class GetRedisSourceNodes(computeReshardTableMaker: ActorRefFactory => ActorRef)
                         (implicit clusterOperations: ClusterOperations, executionContext: ExecutionContext)
  extends Actor with ActorLogging {

  import Overseer._
  import akka.pattern.pipe

  val computeReshardTable = computeReshardTableMaker(context)

  override def receive: Receive = {
    case reshard: ReshardWithNewMaster => getRedisSourceNodes(reshard)
  }

  private def getRedisSourceNodes(reshard: ReshardWithNewMaster) = {
    clusterOperations.getRedisSourceNodes(reshard.uri) map { sourceNodes =>
      (reshard.uri, sourceNodes)
    } pipeTo computeReshardTable
  }

}

object ComputeReshardTable {
  def props(implicit reshardTable: ReshardTableNew): Props = Props(new ComputeReshardTable)

  val name = "computeReshardTable"
}

class ComputeReshardTable(implicit reshardTable: ReshardTableNew) extends Actor with ActorLogging {
  import Overseer._
  import ComputeReshardTable._

  override def receive: Receive = {
    case (uri: RedisURI, sourceNodes: List[RedisClusterNode]) =>
      log.info(s"Received source nodes")

  }
}
