version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "Assignment_3"
  )
libraryDependencies += "org.apache.spark" %% "spark-core"%"2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql"%"2.4.4"
