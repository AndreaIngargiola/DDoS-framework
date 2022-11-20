package it.pps.ddos.devices.actuator

import akka.actor.typed.ActorRef

trait LateInit:
    def setActorRef[T](ref: ActorRef[Message[T]]): Unit
