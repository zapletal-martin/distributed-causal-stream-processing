name := "distributed-causal-stream-processing"

version := "1.0"

scalaVersion := "2.11.8"

lazy val distributedCausalStreamProcessingKafka =
  Project(
    "distributed-causal-stream-processing-kafka",
    file("distributed-causal-stream-processing-kafka"))

lazy val root = project.in(file("."))
  .aggregate(distributedCausalStreamProcessingKafka)