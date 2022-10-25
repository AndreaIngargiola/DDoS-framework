ThisBuild / version := "0.1.0"
ThisBuild / organization := "it.unibo.pps.ddos"

name := "ddos-framework"

scalaVersion := "3.1.3"

lazy val app = (project in file("app"))
  .settings(
      //assembly / mainClass := Some("it.unibo.pps.launcher.Launcher")
  )

lazy val utils = (project in file("utils"))
  .settings(
      assembly / assemblyJarName := s"ddos-framework-$version.jar"
  )

ThisBuild / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
}
val AkkaVersion = "2.6.20"

//Add Monix dependencies
libraryDependencies += "io.monix" %% "monix" % "3.4.1"

//Add JFreeChart dependencies
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"

//Add ScalaTest dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % Test

//Add Prolog dependencies
libraryDependencies += "it.unibo.alice.tuprolog" % "2p-core" % "4.1.1"
libraryDependencies += "it.unibo.alice.tuprolog" % "2p-ui" % "4.1.1"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
)

libraryDependencies += "org.scala-graph" %% "graph-core" % "1.13.5"