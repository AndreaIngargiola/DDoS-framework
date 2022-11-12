package devices.actuators

import scala.collection.immutable.{ListMap}

case class State(name: String):
  def --(message: Class[_]) = new FSM(Option(this), Option(message), ListMap[(State, Class[_]), State]())

case class FSM(val provState: Option[State], val provMessage: Option[Class[_]], val map: ListMap[(State, Class[_]), State]) :
  def ->(s: State) = provMessage match
    case None => new FSM(Option(s), provMessage, map)
    case _ => new FSM(Option(s), Option.empty, map ++ ListMap((provState.get, provMessage.get) -> s))
  def --(m: Class[_]) = new FSM(provState, Option(m), map)
  def _U(fsm: FSM) = new FSM(provState, provMessage, map ++ fsm.map)
  def apply(s: State, m: Class[_]): State = map.getOrElse((s,m), s)
  override def toString: String = map.toString


object main extends App:
  class Message1
  class Message2
  class Message3
  val A = new State("A")
  val msg1 = classOf[Message1]
  val B = new State("B")
  val msg2 =  classOf[Message2]
  val msg3 = classOf[Message3]
  val C = new State("C")
  val FSM = A -- msg1 -> B _U B -- msg2 -> A _U A -- msg3 -> C


  println(FSM(B, msg1))


//modo di costruire la fsm: A -- msg1 -> B -- msg2 -> C
