ThisBuild / version := "0.1.0"
ThisBuild / organization := "it.unibo.pps.ddos"

name := "ddos-framework"

scalaVersion := "3.2.1"

enablePlugins(AkkaGrpcPlugin)

ThisBuild / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
}
val AkkaVersion = "2.7.0"

resolvers += "jitpack" at "https://jitpack.io"

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
    "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion,
)

libraryDependencies += "com.github.Filocava99" % "TuSoW" % "0.8.3"

