package it.pps.ddos.deployment.graph

import akka.actor.typed
import akka.actor.typed.Behavior
import it.pps.ddos.device.actuator.{Actuator, Message}

import scala.::
import scala.annotation.targetName
import scala.collection.mutable
import scala.collection.immutable
import scala.language.postfixOps


object Graph:
    def apply[T](edges: (T, T)*): Any =
        val map = edges.foldLeft(mutable.Map[T, List[T]]())((map, edge) =>
          val list: List[T]  = map.getOrElse(edge._1, List()) :: List[T](edge._2)
            map.put(edge._1)
        ).toMap
        new Graph(edges.head._1, map)


case class Graph[T](initialNode: T, edges: Map[T, List[T]])

implicit final class EdgeAssoc[N1](val n1: N1) extends AnyVal {
    @inline
    @targetName("edgeTo")
    def ->[N >: N1](n2: N): (N1, N) = Tuple2(n1, n2)
}

case class Node[T](name: String, devices: Seq[Behavior[Message[T]]])

object Main extends App :
    Graph[String](
        "A" -> "B",
        "A" -> "C",
        "B" -> "D",
        "C" -> "D",
        "D" -> "A"
    )