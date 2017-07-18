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

  trait ClusterReadyConfig {
    val executionContext: ExecutionContext
    val maxNrRetries: Int

    /**
      * Time in seconds to wait before checking if cluster is ready yet
      */
    val backOffTime: Int
  }


//  object ReshardClusterConfigImpl extends ReshardClusterConfig {
//    val maxNrRetries: Int = -1 // TODO (note: -1 means infinite retries)
//  }

}
