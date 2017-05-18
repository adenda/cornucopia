package com.github.kliewkliew.cornucopia.redis

import org.slf4j.LoggerFactory
import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

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

    val sortedSources = logicalNodes.sorted(Ordering.by((_: LogicalNode).slots.size).reverse)

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

  private def printSortedSources(sources: List[LogicalNode]): Unit = {
    logger.debug(s"Reshard table sorted source slots:")
    sources.foreach(n => logger.debug(s"${n.node.getNodeId} has ${n.slots.size} slots: ${n.slots}"))
  }

}
