import Dependencies._

lazy val `fs2-kafka-wrapper` = (project in file("."))
  .settings(
    name := "fs2-kafka-wrapper",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := "2.12.3",

    scalacOptions ++= Seq(
      "-feature",
      "-deprecation",
      "-language:implicitConversions",
      "-language:higherKinds",
      "-language:existentials",
      "-language:postfixOps",
      "-Xfatal-warnings",
      "-Yno-adapted-args",
      "-Ywarn-value-discard",
      "-Ywarn-unused-import"
    ),

    libraryDependencies ++= Seq(
      fs2Core,
      kafkaClients,

      scalaTest,
      scalaCheck
    )
  )
        