package it.pps.ddos.devices.actuators

import akka.actor.typed.ActorRef

trait LateInit:
    def setActorRef[T](ref: ActorRef[Message[T]]): Unit
