import sbt._
import Versions.{akkaVersion, scalaTestVersion}

object Dependencies {
  lazy val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion % "provided"
  lazy val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion % "provided"
  lazy val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
}
