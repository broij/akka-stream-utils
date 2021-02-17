import Dependencies._

ThisBuild / organization := "be.broij"
ThisBuild / organizationName := "broij"
ThisBuild / organizationHomepage := Some(url("https://github.com/broij/"))

ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/broij/akka-stream-utils"), "scm:git@github.com:broij/akka-stream-utils.git")
)

ThisBuild / developers := List(
  Developer(id = "broij",name = "Julien Broi", email = "julien.broi@gmail.com", url = url("http://www.broij.be"))
)

ThisBuild / description := "A set of operators for akka-streams."
ThisBuild / licenses := List("MIT" -> new URL("https://mit-license.org/"))
ThisBuild / homepage := Some(url("https://github.com/broij/akka-stream-utils"))

ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / publishArtifact in Test := false

ThisBuild / scalaVersion := "2.13.4"
ThisBuild / crossScalaVersions := List("2.12.13", "2.13.4")
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

lazy val `akka-stream-utils` = (project in file("."))
.settings(
  libraryDependencies ++= Seq(
    akkaActorTyped,
    akkaStream,
    akkaStreamTestKit,
    scalaTest,
  )
)
