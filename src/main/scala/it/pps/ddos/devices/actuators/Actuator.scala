package main.scala.it.pps.ddos.devices.actuators

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors

object Actuator {
    def apply(initialState: State) = new Actuator(initialState.behavior)
}

class Actuator(behavior: Behavior[Class[_]]):
    val actor = ActorSystem(behavior, "Actuator")

    def ! (msg: Class[_]): Unit = actor ! msg


object main extends App:
    val stateB = State()
    val stateC = State()
    val stateA = State() -- "msg1".getClass -> stateB -- "msg2".getClass -> stateC
    stateB -- "msg3".getClass -> stateA
    stateC -- "msg4".getClass -> stateA
    val actuator = Actuator(stateA)


