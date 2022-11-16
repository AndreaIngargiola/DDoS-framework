package it.pps.ddos.devices.actuators

import akka.actor.typed.ActorRef
import it.pps.ddos.devices.actuators.Message

trait Message[T]

case class MessageWithReply[T](message: T, replyTo: ActorRef[Message[T]]) extends Message[T]
case class MessageWithoutReply[T](message: T) extends Message[T]
case class Approved[T]() extends Message[T]
case class Denied[T]() extends Message[T]
case class Timeout[T]() extends Message[T]