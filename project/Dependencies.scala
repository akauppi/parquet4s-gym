import sbt._
//import Keys._

object Dependencies {
  //val tsConfig = "com.typesafe" % "config" % "1.4.2"

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
  val logbackClassic = "ch.qos.logback" % "logback-classic"  % "1.3.0-alpha14"
    // 1.2.11 would work nice, but why not get ready for 1.3 :)

  val akkaVer = "2.6.18"
  val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVer
  val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVer

  // Until Scala 3 (which has better built-in enums)
  val enumeratum = "com.beachape" %% "enumeratum" % "1.7.0"

  // Writing Parquet
  val pqt4sVer = "2.3.0"
  val parquet4s = "com.github.mjakubowski84" %% "parquet4s-core" % pqt4sVer
  val `parquet4s-akka` = "com.github.mjakubowski84" %% "parquet4s-akka" % pqt4sVer
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "3.3.2"

  val scalatest = "org.scalatest" %% "scalatest-flatspec" % "3.2.11"
}
