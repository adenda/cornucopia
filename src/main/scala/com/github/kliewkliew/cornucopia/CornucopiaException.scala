package com.github.kliewkliew.cornucopia

object CornucopiaException {
  sealed trait CornucopiaException {
    self: Throwable =>
    val message: String
    val reason: Throwable
  }

  case class CornucopiaRedisConnectionException(message: String, reason: Throwable = None.orNull)
    extends Exception(message, reason) with CornucopiaException
}

