name := "cornucopia"
organization := "com.github.kliewkliew"

//version := "1.1.2"
version := "0.15-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/service/repositories/releases/"
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

val akkaVersion = "2.4.17"

val testDependencies = Seq(
  "com.typesafe.akka" %%  "akka-testkit" % akkaVersion  % "test",
  "org.scalatest"     %%  "scalatest"    % "3.0.0"      % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % Test
)

libraryDependencies ++= Seq(
  "biz.paluch.redis" % "lettuce" % "5.0.0.Beta1",
  "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.8.0",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-RC1",
//  "com.github.kliewkliew" %% "salad" % "0.11.01",
  "com.adenda" %% "salad" % "0.11.03",
  "org.slf4j" % "slf4j-log4j12" % "1.7.22"
) ++ testDependencies
