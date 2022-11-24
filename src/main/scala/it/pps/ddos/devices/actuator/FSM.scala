package main.scala.it.pps.ddos.devices.actuators

import it.pps.ddos.devices.actuator.State

import scala.annotation.targetName
import scala.collection.immutable.ListMap

case class FSM[T](val fromState: Option[State[T]], val fromMessage: Option[T], val map: ListMap[(State[T], T), State[T]]) :
  @targetName("addTransition")
  def ->(s: State[T]): FSM[T] = fromMessage match
    case None => FSM(Option(s), fromMessage, map)
    case _ => FSM(Option(s), Option.empty, map ++ ListMap((fromState.get, fromMessage.get) -> s))
    
  @targetName("addMessage")
  def --(m: T): FSM[T] = FSM(fromState, Option(m), map)
  
  @targetName("joinWith")
  def _U(fsm: FSM[T]): FSM[T] = FSM(fsm.fromState, fsm.fromMessage, map ++ fsm.map)
  
  def apply(s: State[T], m: T): State[T] = map.getOrElse((s,m), s)
  
  override def toString: String = map.toString
  
  def getInitialState: State[T] = map.head._1._1
