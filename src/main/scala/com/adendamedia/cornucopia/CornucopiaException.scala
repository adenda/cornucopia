package com.adendamedia.cornucopia

import com.adendamedia.cornucopia.actors.Overseer._

object CornucopiaException {
  sealed trait CornucopiaException {
    self: Throwable =>
    val message: String
    val reason: Throwable
  }

  // TODO: Remove this, it's only here while re-working and actorizing the stuff
  @SerialVersionUID(1L)
  case class CornucopiaRedisConnectionException(message: String, reason: Throwable = None.orNull)
    extends Exception(message, reason) with CornucopiaException with Serializable

  @SerialVersionUID(1L)
  case class FailedOverseerCommand(overseerCommand: OverseerCommand) extends Exception with Serializable

  @SerialVersionUID(1L)
  case class FailedAddingRedisNodeException(message: String) extends Exception(message: String) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotsException(command: OverseerCommand, reason: Option[Throwable])
    extends Exception with Serializable

  @SerialVersionUID(1L)
  case class KillMeNow(message: String = "Kill me now") extends Exception(message: String) with Serializable
}

