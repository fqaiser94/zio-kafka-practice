version := "1.0.0"
organization := "com.fqaiser94"
scalaVersion := "2.12.10"

val kafkaVersion = "2.6.0"
val zioVersion = "1.0.3"
libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-kafka" % "0.12.1",

  "dev.zio" %% "zio-test" % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,
  "com.dimafeng" %% "testcontainers-scala-kafka" % "0.38.4")

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

Global / onChangedBuildSource := ReloadOnSourceChanges
assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}