package it.pps.ddos.devices.actuators

import akka.actor.typed.Behavior
import it.pps.ddos.devices.actuators.Message

trait State[T](val name: String):
    def getBehavior: Behavior[Message[T]]