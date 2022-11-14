package main.scala.it.pps.ddos.devices.actuators

import scala.collection.immutable.ListMap
//
//class FSM(val fromState: Option[State], val fromMessage: Option[Class[_]], val map: ListMap[(State, Class[_]), State]) :
//  def ->(s: State) = fromMessage match
//    case None => new FSM(Option(s), fromMessage, map)
//    case _ => new FSM(Option(s), Option.empty, map ++ ListMap((fromState.get, fromMessage.get) -> s))
//  def --(m: Class[_]) = new FSM(fromState, Option(m), map)
//  def _U(fsm: FSM) = new FSM(fromState, fromMessage, map ++ fsm.map)
//  def apply(s: State, m: Class[_]): State = map.getOrElse((s,m), s)
//  override def toString: String = map.toString
//  def getFirstState: State = ???
//
//
//object main extends App:
//  class Message1
//  class Message2
//  class Message3
//  var FSM = new FSM(Option.empty, Option.empty, ListMap.empty)
//  val A = State("A", FSM)
//  val msg1 = classOf[Message1]
//  val B = State("B", FSM)
//  val msg2 =  classOf[Message2]
//  val msg3 = classOf[Message3]
//  val C = State("C", FSM)
//  val FSM = A -- msg1 -> B _U B -- msg2 -> A _U A -- msg3 -> C
//
//
//  println(FSM(B, msg1))
//

//modo di costruire la fsm: A -- msg1 -> B -- msg2 -> C
