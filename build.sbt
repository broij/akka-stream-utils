import Dependencies._

ThisBuild / organization := "be.broij"
ThisBuild / version := "0.0.0"
ThisBuild / scalaVersion := "2.13.4"
ThisBuild / organizationName := "broij"
ThisBuild / publishArtifact in (Test, packageBin) := true

lazy val `akka-stream-utils` = (project in file("."))
.settings(
  libraryDependencies ++= Seq(
    akkaActorTyped,
    akkaStream,
    akkaStreamTestKit,
    scalaTest,
  )
)
