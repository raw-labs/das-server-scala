import sbt.*
import sbt.Keys.*

ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "raw-labs",
  sys.env.getOrElse("GITHUB_TOKEN", ""))

lazy val commonSettings = Seq(
  homepage := Some(url("https://www.raw-labs.com/")),
  organization := "com.raw-labs",
  organizationName := "RAW Labs SA",
  organizationHomepage := Some(url("https://www.raw-labs.com/")),
  // Use cached resolution of dependencies
  // http://www.scala-sbt.org/0.13/docs/Cached-Resolution.html
  updateOptions := updateOptions.in(Global).value.withCachedResolution(true),
  resolvers += "RAW Labs GitHub Packages" at "https://maven.pkg.github.com/raw-labs/_")

lazy val buildSettings = Seq(scalaVersion := "2.13.15")

lazy val compileSettings = Seq(
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / mappings := Seq(),
  Compile / packageSrc / publishArtifact := true,
  Compile / packageDoc / publishArtifact := false,
  Compile / packageBin / packageOptions += Package.ManifestAttributes(
    "Automatic-Module-Name" -> name.value.replace('-', '.')),
  // Ensure Java-based DAS SDK code is compiled first, so it is accessible from Scala.
  compileOrder := CompileOrder.JavaThenScala,
  // Ensure we fork new JVM for run, so we can set JVM flags.
  Compile / run / fork := true)

lazy val testSettings = Seq(
  // Ensure we fork new JVM for run, so we can set JVM flags.
  Test / fork := true,
  // Required for publishing test artifacts.
  Test / publishArtifact := true)

val isCI = sys.env.getOrElse("CI", "false").toBoolean

lazy val publishSettings = Seq(
  versionScheme := Some("early-semver"),
  publish / skip := false,
  publishMavenStyle := true,
  publishTo := Some(
    "GitHub raw-labs Apache Maven Packages" at "https://maven.pkg.github.com/raw-labs/das-server-scala"),
  publishConfiguration := publishConfiguration.value.withOverwrite(isCI))

lazy val strictBuildSettings =
  commonSettings ++ compileSettings ++ buildSettings ++ testSettings ++ Seq(scalacOptions ++= Seq("-Xfatal-warnings"))

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "das-server-scala",
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion),
    buildInfoPackage := "com.rawlabs.das.server",
    strictBuildSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      // Logging
      // Using SLF4j as the facade, Logback as the implementation, Loki for log aggregation and with Scala support.
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.5.12",
      "org.slf4j" % "slf4j-api" % "2.0.16",
      "org.slf4j" % "log4j-over-slf4j" % "2.0.16",
      "org.slf4j" % "jcl-over-slf4j" % "2.0.16",
      "org.slf4j" % "jul-to-slf4j" % "2.0.16",
      "com.github.loki4j" % "loki-logback-appender" % "1.5.2",
      // Configuration
      "com.typesafe" % "config" % "1.4.3",
      // Protocol DAS
      "com.raw-labs" %% "protocol-das" % "1.0.0",
      // Akka Streams
      "org.apache.pekko" %% "pekko-actor-typed" % "1.1.3",
      "org.apache.pekko" %% "pekko-stream" % "1.1.3",
      "org.apache.pekko" %% "pekko-http" % "1.1.0",
      // Jackson databind
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2" % Test,
      // gRPC Testing
      "io.grpc" % "grpc-inprocess" % "1.62.2",
      // Web UI
      "com.typesafe.akka" %% "akka-http" % "10.5.3",
      "com.lihaoyi" %% "scalatags" % "0.13.1",
      // Postgres
      "org.postgresql" % "postgresql" % "42.7.4" % Test,
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.19" % Test))
