import sbt.*
import sbt.Keys.*

import com.typesafe.sbt.packager.docker.{Cmd, LayeredMapping}

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

lazy val dockerSettings = Seq(
  Docker / packageName := "das-mock-server",
  dockerBaseImage := "eclipse-temurin:21-jre",
  dockerLabels ++= Map(
    "vendor" -> "RAW Labs SA",
    "product" -> "das-mock-server",
    "image-type" -> "final",
    "org.opencontainers.image.source" -> "https://github.com/raw-labs/das-server-scala"),
  Docker / daemonUser := "raw",
  Docker / daemonUserUid := Some("1001"),
  Docker / daemonGroup := "raw",
  Docker / daemonGroupGid := Some("1001"),
  dockerExposedVolumes := Seq("/var/log/raw"),
  dockerExposedPorts := Seq(50051),
  dockerEnvVars := Map("PATH" -> s"${(Docker / defaultLinuxInstallLocation).value}/bin:$$PATH"),
  dockerEnvVars += "LANG" -> "C.UTF-8",
  updateOptions := updateOptions.value.withLatestSnapshots(true),
  Linux / linuxPackageMappings += packageTemplateMapping(s"/var/lib/${packageName.value}")(),
  bashScriptDefines := {
    val ClasspathPattern = "declare -r app_classpath=\"(.*)\"\n".r
    bashScriptDefines.value.map {
      case ClasspathPattern(classpath) => s"""
                                             |declare -r app_classpath="$${app_home}/../conf:$classpath"
                                             |""".stripMargin
      case _ @entry => entry
    }
  },
  Docker / dockerLayerMappings := (Docker / dockerLayerMappings).value.map {
    case lm @ LayeredMapping(Some(1), file, path) => {
      val fileName = java.nio.file.Paths.get(path).getFileName.toString
      if (!fileName.endsWith(".jar")) {
        // If it is not a jar, put it on the top layer. Configuration files and other small files.
        LayeredMapping(Some(2), file, path)
      } else if (fileName.startsWith("com.raw-labs") && fileName.endsWith(".jar")) {
        // If it is one of our jars, also top layer. These will change often.
        LayeredMapping(Some(2), file, path)
      } else {
        // Otherwise it is a 3rd party library, which only changes when we change dependencies, so leave it in layer 1
        lm
      }
    }
    case lm @ _ => lm
  },
  dockerAlias := dockerAlias.value.withTag(Option(version.value.replace("+", "-"))),
  dockerAliases := {
    val devRegistry = sys.env.getOrElse("DEV_REGISTRY", "ghcr.io/raw-labs/das-server-scala")
    val releaseRegistry = sys.env.get("RELEASE_DOCKER_REGISTRY")
    val baseAlias = dockerAlias.value.withRegistryHost(Some(devRegistry))

    releaseRegistry match {
      case Some(releaseReg) => Seq(baseAlias, dockerAlias.value.withRegistryHost(Some(releaseReg)))
      case None             => Seq(baseAlias)
    }
  })

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
      "com.raw-labs" %% "protocol-das" % "1.0.1",
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
      "org.scalatest" %% "scalatest" % "3.2.19" % Test),
    dockerSettings)

lazy val printDockerImageName = taskKey[Unit]("Prints the full Docker image name that will be produced")

printDockerImageName := {
  // Get the main Docker alias (the first one in the sequence)
  val alias = (Docker / dockerAliases).value.head
  // The toString method already returns the full image name with registry and tag
  println(s"DOCKER_IMAGE=${alias}")
}
