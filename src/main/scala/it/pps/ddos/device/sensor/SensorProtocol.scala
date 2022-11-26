package it.pps.ddos.device.sensor

import akka.actor.typed.ActorRef

/* Definition of the message protocol shared by the sensors */
object SensorProtocol:
  trait Message

  case class PropagateStatus(requester: ActorRef[Message]) extends Message

  case class Status[T](author: ActorRef[Message], value: T) extends Message

  case class UpdateStatus[T](value: T) extends Message

  case class Subscribe(replyTo: ActorRef[Message]) extends Message

  case class SubscribeAck(author: ActorRef[Message]) extends Message

  case class Unsubscribe(replyTo: ActorRef[Message]) extends Message

  case class UnsubscribeAck(author: ActorRef[Message]) extends Message