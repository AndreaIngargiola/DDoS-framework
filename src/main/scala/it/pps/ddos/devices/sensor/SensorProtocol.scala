package it.pps.ddos.devices.sensor

import akka.actor.typed.ActorRef

/* Definition of the message protocol shared by the sensors  */
object SensorProtocol:
  trait Message
  case class PropagateStatus(requester: ActorRef[Message]) extends Message
  case class Status[T](author: ActorRef[Message], value: T) extends Message
  case class UpdateStatus[T](value: T) extends Message