package com.adendamedia.cornucopia

import com.adendamedia.cornucopia.actors.Overseer._

object CornucopiaException {
  sealed trait CornucopiaException {
    self: Throwable =>
    val message: String
    val reason: Throwable
  }

  @SerialVersionUID(1L)
  case class CornucopiaRedisConnectionException(message: String, reason: Throwable = None.orNull)
    extends Exception(message, reason) with CornucopiaException with Serializable

  // TODO: Give this a message so that error logs are descriptive
  @SerialVersionUID(1L)
  case class FailedOverseerCommand(message: String = "", overseerCommand: OverseerCommand, reason: Option[Throwable] = None)
    extends Exception(message: String) with Serializable

  @SerialVersionUID(1L)
  case class FailedAddingRedisNodeException(message: String) extends Exception(message: String) with Serializable

  @SerialVersionUID(1L)
  case class FailedWaitingForClusterToBeReadyException(message: String) extends Exception(message: String) with Serializable

  @SerialVersionUID(1L)
  case class MigrateSlotsException(message: String, command: OverseerCommand, reason: Option[Throwable])
    extends Exception(message: String) with Serializable

  @SerialVersionUID(1L)
  case class KillMeNow(message: String = "Kill me now") extends Exception(message: String) with Serializable
}

