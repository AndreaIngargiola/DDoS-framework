package main.scala.it.pps.ddos.devices.actuators

import it.pps.ddos.devices.actuator.State

import scala.collection.immutable.ListMap

class FSM[T](val fromState: Option[State[T]], val fromMessage: Option[T], val map: ListMap[(State[T], T), State[T]]) :
  def ->(s: State[T]) = fromMessage match
    case None => new FSM(Option(s), fromMessage, map)
    case _ => new FSM(Option(s), Option.empty, map ++ ListMap((fromState.get, fromMessage.get) -> s))
  def --(m: T) = new FSM(fromState, Option(m), map)
  def _U(fsm: FSM[T]) = new FSM(fromState, fromMessage, map ++ fsm.map)
  def apply(s: State[T], m: T): State[T] = map.getOrElse((s,m), s)
  override def toString: String = map.toString
  def getInitialState: State[T] = map.head._2



//object main extends App:
//  class Message1
//  class Message2
//  class Message3
//  var FSM = new FSM(Option.empty, Option.empty, ListMap.empty)
//  val A = BasicState("A")
//  val msg1 = classOf[Message1]
//  val B = BasicState("B")
//  val msg2 =  classOf[Message2]
//  val msg3 = classOf[Message3]
//  val C = BasicState("C")
//  val FSM = A -- msg1 -> B _U B -- msg2 -> A _U A -- msg3 -> C
//
//
//  println(FSM(B, msg1))


//modo di costruire la fsm: A -- msg1 -> B -- msg2 -> C
