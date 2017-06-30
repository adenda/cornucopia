package com.adendamedia.cornucopia.redis

trait Operation {
  def key: String
  def ordinal: Int
}

object UNSUPPORTED extends Operation {
  val key = "dummy"
  val ordinal = 0
}

// Event partition operations.

object ADD_MASTER extends Operation {
  val key = "+master"
  val ordinal = UNSUPPORTED.ordinal + 1
}

object ADD_SLAVE extends Operation {
  val key = "+slave"
  val ordinal = ADD_MASTER.ordinal + 1
}

object RESHARD extends Operation {
  val key = "*reshard"
  val ordinal = ADD_SLAVE.ordinal + 1
}

object CLUSTER_TOPOLOGY extends Operation {
  val key = "?topology"
  val ordinal = RESHARD.ordinal + 1
}

// Node removal partition operations.

object REMOVE_MASTER extends Operation {
  val key = "-master"
  val ordinal = CLUSTER_TOPOLOGY.ordinal + 1
}

object REMOVE_SLAVE extends Operation {
  val key = "-slave"
  val ordinal = REMOVE_MASTER.ordinal + 1
}
