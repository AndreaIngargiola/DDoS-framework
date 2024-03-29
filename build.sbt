ThisBuild / version := "0.1.0"
ThisBuild / organization := "it.unibo.pps.ddos"

name := "ddos-framework"

scalaVersion := "3.2.1"

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
val AkkaVersion = "2.7.0"

//Add Monix dependencies
libraryDependencies += "io.monix" %% "monix" % "3.4.1"

//Add JFreeChart dependencies
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.3"

//Add ScalaTest dependencies
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % Test

//Add Prolog dependencies
libraryDependencies += "it.unibo.alice.tuprolog" % "2p-core" % "4.1.1"
libraryDependencies += "it.unibo.alice.tuprolog" % "2p-ui" % "4.1.1"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test
)