name := "JuveStreams"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.0"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.7"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"
libraryDependencies += "com.danielasfregola" %% "twitter4s" % "6.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "2.4.4"
libraryDependencies += "org.slf4s" %% "slf4s-api" % "1.7.25"
