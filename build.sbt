name := "cornucopia"
organization := "com.github.kliewkliew"

version := "1.1.1"

scalaVersion := "2.11.8"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/service/repositories/releases/"
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "biz.paluch.redis" % "lettuce" % "5.0.0.Beta1",
  "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.8.0",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-RC1",
  "com.github.kliewkliew" %% "salad" % "0.10.3",
  "org.slf4j" % "slf4j-log4j12" % "1.7.22"
)
