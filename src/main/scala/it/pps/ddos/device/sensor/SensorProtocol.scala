package it.pps.ddos.device.sensor

import akka.actor.typed.ActorRef

/* Definition of the message protocol shared by the sensors  */
object SensorProtocol:
  trait Message
  final case class PropagateStatus(requester: ActorRef[Message]) extends Message
  final case class Status[T](author: ActorRef[Message], value: T) extends Message
  final case class UpdateStatus[T](value: T) extends Message