package com.adendamedia.cornucopia.redis

trait Operation {
  def key: String
  val message: String
  def ordinal: Int
}

object UNSUPPORTED extends Operation {
  val key = "dummy"
  val message = "Invalid"
  val ordinal = 0
}

// Event partition operations.

object ADD_MASTER extends Operation {
  val key = "+master"
  val message = "Add master"
  val ordinal = UNSUPPORTED.ordinal + 1
}

object ADD_SLAVE extends Operation {
  val key = "+slave"
  val message = "Add slave"
  val ordinal = ADD_MASTER.ordinal + 1
}

object RESHARD extends Operation {
  val key = "*reshard"
  val message = "Reshard"
  val ordinal = ADD_SLAVE.ordinal + 1
}

object CLUSTER_TOPOLOGY extends Operation {
  val key = "?topology"
  val message = "Cluster topology"
  val ordinal = RESHARD.ordinal + 1
}

// Node removal partition operations.

object REMOVE_MASTER extends Operation {
  val key = "-master"
  val message = "Remove master"
  val ordinal = CLUSTER_TOPOLOGY.ordinal + 1
}

object REMOVE_SLAVE extends Operation {
  val key = "-slave"
  val message = "Remove slave"
  val ordinal = REMOVE_MASTER.ordinal + 1
}
