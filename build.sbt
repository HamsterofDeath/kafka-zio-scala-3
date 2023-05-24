ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "events"
  )


libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "2.0.10",
  "dev.zio" %% "zio-streams" % "2.0.10",
  "dev.zio" %% "zio-kafka" % "2.1.3",
  "org.apache.kafka" % "kafka-clients" % "3.4.0"
)
