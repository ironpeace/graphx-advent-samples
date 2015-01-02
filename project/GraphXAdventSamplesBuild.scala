import sbt._
import sbt.Keys._

object GraphXAdventSamplesBuild extends Build {
  lazy val graphxsamples = Project(
    id = "graphx-advent-samples",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "graphx-advent-samples",
      organization := "com.teppeistudio",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.4",
      libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0",
      libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.1.0",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.1.0",
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
    )
  )
}
