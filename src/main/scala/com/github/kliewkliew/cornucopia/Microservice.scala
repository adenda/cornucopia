package com.github.kliewkliew.cornucopia

class Microservice {
  def main(args: Array[String]): Unit = {
    new kafka.Consumer().run
  }
}