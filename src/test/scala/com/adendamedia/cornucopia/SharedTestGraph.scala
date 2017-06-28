package com.adendamedia.cornucopia

import com.adendamedia.cornucopia.graph.CornucopiaActorSource

object SharedTestGraph {
  lazy val graph = new CornucopiaActorSource
  lazy val redisCommandRouter = graph.redisCommandRouter
}
