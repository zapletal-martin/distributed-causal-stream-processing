name := "distributed-causal-stream-processing-kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.7",
  "org.apache.kafka" %% "kafka" % "0.10.0.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
  "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test"
)