import sbt._

object Dependencies {
  lazy val fs2Core = "co.fs2" %% "fs2-core" % fs2Version
  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion

  lazy val scalaTest =  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test

  val fs2Version = "0.9.7"
  val kafkaClientsVersion = "0.11.0.1"
  val scalaTestVersion = "3.0.4"
  val scalaCheckVersion = "1.13.4"
}