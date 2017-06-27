package com.adendamedia.cornucopia.redis

import org.slf4j.LoggerFactory
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

import scala.annotation.tailrec

object ReshardTable {

  type NodeId = String
  type Slot = Int
  type ReshardTableType = scala.collection.immutable.Map[NodeId, List[Slot]]

  case class ReshardTableException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  case class LogicalNode(node: RedisClusterNode, slots: List[Int])

  private val logger = LoggerFactory.getLogger(this.getClass)

  def computeReshardTable(sourceNodes: List[RedisClusterNode])
                         (implicit ExpectedTotalNumberSlots: Int): ReshardTableType = {
    import scala.collection.JavaConverters._

    val logicalNodes = sourceNodes.map { n =>
      val slots = n.getSlots.asScala.toList.map(_.toInt)
      LogicalNode(n, slots)
    }

    val sortedSources = logicalNodes.sorted(Ordering.by((_: LogicalNode).slots.size).reverse) // descending order

    printSortedSources(sortedSources)

    val totalSourceSlots = sortedSources.foldLeft(0)((sum, n) => sum + n.slots.size)

    logger.debug(s"Reshard table total sources: $totalSourceSlots")

    if (totalSourceSlots != ExpectedTotalNumberSlots) {
      throw ReshardTableException(s"Reshard table total source slots is $totalSourceSlots, but is not equal to expected number $ExpectedTotalNumberSlots")
    }

    val numSlots = totalSourceSlots / (logicalNodes.size + 1) // total number of slots to move to target

    logger.debug(s"Reshard table total number of slots to move to target: $numSlots")

    def computeNumSlots(i: Int, source: LogicalNode): Int = {
      if (i == 0) Math.ceil((numSlots.toFloat / totalSourceSlots) * source.slots.size).toInt
      else Math.floor((numSlots.toFloat / totalSourceSlots) * source.slots.size).toInt
    }

    val reshardTable: ReshardTableType = Map.empty[NodeId, List[Slot]]

    val table = sortedSources.zipWithIndex.foldLeft(reshardTable) { case (tbl, (source, i)) =>
      val sortedSlots = source.slots.sorted
      val n = computeNumSlots(i, source)
      val slots = sortedSlots.take(n)
      val nodeId = source.node.getNodeId
      logger.debug(s"Reshard table adding $n slots from $nodeId to move to target")
      tbl + (nodeId -> slots)
    }

    table
  }

  def computeReshardTablePrime(retiredNode: RedisClusterNode, remainingNodes: List[RedisClusterNode])
                              (implicit ExpectedTotalNumberSlots: Int): ReshardTableType = {
    import scala.collection.JavaConverters._

    val remainingLogicalNodes = remainingNodes.map { n =>
      val slots = n.getSlots.asScala.toList.map(_.toInt)
      LogicalNode(n, slots)
    }

    val retiredNodeSlots = retiredNode.getSlots.asScala.toList.map(_.toInt)
    val retiredLogicalNode: LogicalNode = LogicalNode(retiredNode, retiredNodeSlots)

    // TODO: Maybe this should be in normal order? Check redis-trib
    val sortedTargets = remainingLogicalNodes.sorted(Ordering.by((_: LogicalNode).slots.size).reverse) // descending order

    printSortedTargets(sortedTargets)
    printRetiredNode(retiredLogicalNode)

    val totalSlots = sortedTargets.foldLeft(retiredLogicalNode.slots.size)((sum, n) => sum + n.slots.size)

    logger.debug(s"Reshard table total slots: $totalSlots")

    if (totalSlots != ExpectedTotalNumberSlots) {
      throw ReshardTableException(s"Reshard table total slots is $totalSlots, but is not equal to expected number $ExpectedTotalNumberSlots")
    }

    val numSlots = retiredLogicalNode.slots.size // total number of slots to move to targets

    logger.debug(s"Reshard table total number of slots to move to targets from retiring master: $numSlots")

    // compute the number of slots to move from retired node to the i-th smallest remaining node
    // TODO: This should maybe be the i-th largest (see TODO above on sortedTargets)
    def computeNumSlots(i: Int): Int = {
      if (i == 0) Math.ceil(numSlots.toFloat / sortedTargets.length).toInt
      else Math.floor(numSlots.toFloat / sortedTargets.length).toInt
    }

    @tailrec
    def computeReshardTable(tbl: ReshardTableType, remainingSourceSlots: List[Int], i: Int): ReshardTableType = {
      if (remainingSourceSlots.isEmpty) tbl
      else {
        val numSlots = computeNumSlots(i)
        val slots = remainingSourceSlots.take(numSlots)
        val slotsRemaining = remainingSourceSlots.drop(numSlots)
        val targetNodeId = sortedTargets(i).node.getNodeId
        val newTbl = tbl + (targetNodeId -> slots)
        computeReshardTable(newTbl, slotsRemaining, i + 1)
      }
    }

    val reshardTable: ReshardTableType = Map.empty[NodeId, List[Slot]]

    computeReshardTable(reshardTable, retiredLogicalNode.slots.sorted, 0)
  }

  private def printRetiredNode(retiredNode: LogicalNode): Unit = {
    logger.debug(s"Reshard table sorted source slots from retired node:")
    logger.debug(s"${retiredNode.node.getNodeId} has ${retiredNode.slots.size} slots: ${retiredNode.slots}")
  }

  private def printSortedTargets(sources: List[LogicalNode]): Unit = {
    logger.debug(s"Reshard table sorted target slots:")
    sources.foreach(n => logger.debug(s"${n.node.getNodeId} has ${n.slots.size} slots: ${n.slots}"))
  }

  private def printSortedSources(sources: List[LogicalNode]): Unit = {
    logger.debug(s"Reshard table sorted source slots:")
    sources.foreach(n => logger.debug(s"${n.node.getNodeId} has ${n.slots.size} slots: ${n.slots}"))
  }

}
