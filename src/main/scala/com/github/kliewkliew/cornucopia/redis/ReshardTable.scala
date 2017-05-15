package com.github.kliewkliew.cornucopia.redis

import com.lambdaworks.redis.cluster.models.partitions.RedisClusterNode

object ReshardTable {

  type NodeId = String
  type Slot = Int
  type ReshardTable = scala.collection.immutable.Map[NodeId, List[Slot]]

  def computeReshardTable(sourceNodes: List[RedisClusterNode]): ReshardTable = {
    import scala.collection.JavaConverters._

    case class LogicalNode(node: RedisClusterNode, slots: List[Int])

    val logicalNodes = sourceNodes.map { n =>
      val slots = n.getSlots.asScala.toList.map(_.toInt)
      LogicalNode(n, slots)
    }

    val sortedSources = logicalNodes.sorted(Ordering.by((_: LogicalNode).slots.size).reverse)

    val totalSourceSlots = sortedSources.foldLeft(0)((sum, n) => sum + n.slots.size)

    val numSlots = totalSourceSlots / (logicalNodes.size + 1) // total number of slots to move to target

    def computeNumSlots(i: Int, source: LogicalNode): Int = {
      if (i == 0) Math.ceil((numSlots.toFloat / totalSourceSlots) * source.slots.size).toInt
      else Math.floor((numSlots.toFloat / totalSourceSlots) * source.slots.size).toInt
    }

    val reshardTable: ReshardTable = Map.empty[NodeId, List[Slot]]

    val table = sortedSources.zipWithIndex.foldLeft(reshardTable) { case (tbl, (source, i)) =>
      val sortedSlots = source.slots.sorted
      val n = computeNumSlots(i, source)
      val slots = sortedSlots.take(n)
      val nodeId = source.node.getNodeId
      tbl + (nodeId -> slots)
    }

    table
  }
}
