ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "LoadConsumers",
    version := "1.0",
    scalaVersion := "2.12.17"
  )

val sparkVersion = "3.5.1"
val deltaLakeVersion = "3.1.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.delta" %% "delta-spark" % deltaLakeVersion,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)
