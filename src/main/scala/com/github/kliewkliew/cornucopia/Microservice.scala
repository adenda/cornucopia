package com.github.kliewkliew.cornucopia

object Microservice {
  def main(args: Array[String]): Unit = {
    new graph.CornucopiaKafkaSource().run
  }
}