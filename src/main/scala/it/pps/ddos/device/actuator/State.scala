package it.pps.ddos.device.actuator

import akka.actor.typed.Behavior

import scala.collection.immutable.ListMap
import it.pps.ddos.device.DeviceProtocol._

trait State[T](val name: String):
    def getBehavior: Behavior[Message]

    def --(message: T) = new FSM[T](Option(this), Option(message), ListMap[(State[T], T), State[T]]())

    def copy(): State[T]