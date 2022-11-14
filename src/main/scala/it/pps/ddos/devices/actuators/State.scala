package main.scala.it.pps.ddos.devices.actuators

import scala.annotation.targetName
import scala.collection.immutable.{HashMap, ListMap}
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object State:
    def apply(): State = new State()

class State():
    var tempMsg: Option[Class[_]] = None
    var map: HashMap[Class[_], State] = HashMap()

    //def apply(s: State, m: Class[_]): State = map.getOrElse((s,m), s)

    val behavior: Behavior[Class[_]] = Behaviors.setup[Class[_]](context =>
        Behaviors.receiveMessage {
            msg =>
                if (map.contains(msg))
                    map(msg).behavior
                else Behaviors.same
        }
    )

    def ->(s: State) = tempMsg match
        case None => throw new Exception("No message defined")
        case _ =>
            map = map + (tempMsg.get -> s)
            this

    def --(m: Class[_]) =
        tempMsg = Option(m)
        this

    //def _U(fsm: FSM) = new FSM(fromState, fromMessage, map ++ fsm.map)