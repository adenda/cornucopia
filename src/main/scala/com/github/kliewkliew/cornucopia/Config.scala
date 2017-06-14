package com.github.kliewkliew.cornucopia

import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.github.kliewkliew.cornucopia.actors.CornucopiaSource
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import scala.concurrent.duration._
import com.github.kliewkliew.cornucopia.actors.SharedActorSystem.sharedActorSystem

object Config {

  object Cornucopia {
    private val config = ConfigFactory.load().getConfig("cornucopia")
    val minReshardWait = config.getInt("reshard.interval").seconds
    val gracePeriod = config.getInt("grace.period") * 1000
    val refreshTimeout = config.getInt("refresh.timeout") * 1000
    val batchPeriod = config.getInt("batch.period").seconds
    val source = config.getString("source")
  }

  implicit val actorSystem = sharedActorSystem

  // Log failures and resume processing
  private val decider: Supervision.Decider = { e =>
    LoggerFactory.getLogger(this.getClass).error("Failed to process event", e)
    Supervision.Resume
  }

  private val materializerSettings = ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)

  implicit val materializer = ActorMaterializer(materializerSettings)(actorSystem)

  val cornucopiaActorProps = CornucopiaSource.props
  val cornucopiaActorSource = Source.actorPublisher[CornucopiaSource.Task](cornucopiaActorProps)

  object ReshardTableConfig {
    final implicit val ExpectedTotalNumberSlots: Int = 16384
  }

  val reshardTimeout: Int = ConfigFactory.load().getConfig("cornucopia").getInt("reshard.timeout")

  val migrateSlotTimeout: Int = ConfigFactory.load().getConfig("cornucopia").getInt("reshard.migrate.slot.timeout")
}
