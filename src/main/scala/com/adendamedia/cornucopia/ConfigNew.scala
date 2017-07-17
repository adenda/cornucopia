package com.adendamedia.cornucopia

import scala.concurrent.ExecutionContext

object ConfigNew {

  trait ReshardClusterConfig {
    val maxNrRetries: Int
    val expectedTotalNumberSlots: Int
    val executionContext: ExecutionContext
  }

  trait ClusterConnectionsConfig {
    /**
      * The maximum number of retries to try and get cluster connections
      */
    val maxNrRetries: Int

    val executionContext: ExecutionContext
  }


//  object ReshardClusterConfigImpl extends ReshardClusterConfig {
//    val maxNrRetries: Int = -1 // TODO (note: -1 means infinite retries)
//  }

}
