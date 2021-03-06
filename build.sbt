name := "cornucopia"

organization := "com.adendamedia"

version := "0.6.2"

scalaVersion := "2.11.8"

enablePlugins(JavaAppPackaging, DockerPlugin)

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
  "com.typesafe.akka" %% "akka-http-core"  % "2.4.11",
  "com.typesafe.akka" %% "akka-http-experimental"  % "2.4.11",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental"  % "2.4.11",
  "com.typesafe.akka" %% "akka-agent" % "2.4.11",
  "com.adendamedia" %% "salad" % "0.9.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.22"
) ++ testDependencies

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

// ------------------------------------------------ //
// ------------- Docker configuration ------------- //
// ------------------------------------------------ //
import NativePackagerHelper._

mappings in Universal ++= directory( baseDirectory.value / "src" / "main" / "resources" )

javaOptions in Universal ++= Seq(
  "-Dconfig.file=etc/container.conf",
  "-Dlog4j.configuration=file:/usr/local/etc/log4j.properties"
)

packageName in Docker := packageName.value

version in Docker := version.value

dockerBaseImage := "openjdk"

dockerRepository := Some("adenda")

defaultLinuxInstallLocation in Docker := "/usr/local"

daemonUser in Docker := "root"