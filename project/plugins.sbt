resolvers += Classpaths.sbtPluginReleases

autoCompilerPlugins := true

addDependencyTreePlugin

libraryDependencies += "commons-io" % "commons-io" % "2.17.0"

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")

addSbtPlugin("com.github.sbt" % "sbt-protobuf" % "0.8.0")

addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")
