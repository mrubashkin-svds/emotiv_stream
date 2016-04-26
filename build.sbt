name := "emotiv_stream"

version := "0.0.1"

scalaVersion := "2.11.7"

lazy val kafkaVersion = "0.9.1.0-cp1"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.scalactic" %% "scalactic" % "3.0.0-M15",
  "org.scalatest" %% "scalatest" % "3.0.0-M15" % "test"
)

resolvers ++= Seq(
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  "Confluent" at "http://packages.confluent.io/maven/"
)

mainClass in assembly := Some("com.svds.emotiv_stream.kafka.streams.StreamTransformer")