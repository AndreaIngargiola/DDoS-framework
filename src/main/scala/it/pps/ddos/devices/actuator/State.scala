package it.pps.ddos.devices.actuator

import akka.actor.typed.Behavior
import main.scala.it.pps.ddos.devices.actuators.FSM

import scala.collection.immutable.ListMap

trait State[T](val name: String):
    def getBehavior: Behavior[Message[T]]

    def --(message: T) = new FSM[T](Option(this), Option(message), ListMap[(State[T], T), State[T]]())