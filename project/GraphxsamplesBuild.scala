import sbt._
import sbt.Keys._

object GraphxsamplesBuild extends Build {

  lazy val graphxsamples = Project(
    id = "graphx-samples",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "graphx-samples",
      organization := "com.teppeistudio",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.4",

      // add other settings here
      libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1",
      libraryDependencies += "org.apache.spark" %% "spark-graphx" % "0.9.1",

      resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
    )
  )
}
