package it.pps.ddos.devices.actuators

import akka.actor.typed.Behavior

trait State[T](val name: String):
    def getBehavior: Behavior[Message[T]]