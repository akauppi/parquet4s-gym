//
// build.sbt
//
import Dependencies._

val defaults = Def.settings(
  scalaVersion := "2.13.8",
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf8",
    "-feature",
    "-unchecked",
    "-language", "postfixOps"
  )
)

val commonDeps = Seq(
  scalaLogging,
  logbackClassic,
  scalatest % Test
)

lazy val root = (project in file("."))
  .settings(
    defaults,
    libraryDependencies ++= commonDeps ++ Seq(
      enumeratum,
      akkaStream,
      `parquet4s-akka`,
      hadoopClient
    )
  )
