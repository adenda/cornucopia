package com.adendamedia.cornucopia

import akka.stream.scaladsl.Source
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.adendamedia.cornucopia.actors.CornucopiaSource
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import com.adendamedia.cornucopia.actors.SharedActorSystem.sharedActorSystem

object Config {

  object Cornucopia {
    private val config = ConfigFactory.load().getConfig("cornucopia")
    val minReshardWait: FiniteDuration = config.getInt("reshard.interval").seconds
    val refreshTimeout: Int = config.getInt("refresh.timeout") * 1000
    val batchPeriod: FiniteDuration = config.getInt("batch.period").seconds
  }

  implicit val actorSystem: ActorSystem = sharedActorSystem

  // Log failures and resume processing
  private val decider: Supervision.Decider = { e =>
    LoggerFactory.getLogger(this.getClass).error("Failed to process event", e)
    Supervision.Resume
  }

  private val materializerSettings: ActorMaterializerSettings =
    ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)

  implicit val materializer: ActorMaterializer = ActorMaterializer(materializerSettings)(actorSystem)

  private val cornucopiaActorProps = CornucopiaSource.props
  val cornucopiaActorSource: Source[CornucopiaSource.Task, ActorRef] =
    Source.actorPublisher[CornucopiaSource.Task](cornucopiaActorProps)

  object ReshardTableConfig {
    final implicit val ExpectedTotalNumberSlots: Int = 16384
  }

  val reshardTimeout: Int = ConfigFactory.load().getConfig("cornucopia").getInt("reshard.timeout")

  val migrateSlotTimeout: Int = ConfigFactory.load().getConfig("cornucopia").getInt("reshard.migrate.slot.timeout")

  val dispatchTaskTimeout: Int = 30 // TODO: config

  val overseerMaxNrRetries: Int = 5 // TODO: config
}
