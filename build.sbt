ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "hdfs-files-management"
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "io.scalaland" %% "chimney" % "0.6.2",
  "ch.qos.logback" % "logback-classic" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)