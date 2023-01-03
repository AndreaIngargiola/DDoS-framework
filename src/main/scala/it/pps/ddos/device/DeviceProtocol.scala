package it.pps.ddos.device

import akka.actor.typed.ActorRef
import scala.collection.immutable.List

/* Definition of the message protocol shared by the sensors */
object DeviceProtocol:
  trait Message
  trait DeviceMessage extends Message
  trait SensorMessage extends DeviceMessage
  trait ActuatorMessage extends DeviceMessage

  abstract class Output[T](author: ActorRef[Message], value:T) extends Message

  case class Status[T](author: ActorRef[Message], value: T) extends Output[T](author, value)

  case class Statuses[T](author: ActorRef[Message], value: List[T]) extends Output[List[T]](author, value)

  case class PropagateStatus(requester: ActorRef[Message]) extends DeviceMessage, ActuatorMessage

  case class UpdateStatus[T](value: T) extends SensorMessage

  case class Subscribe(replyTo: ActorRef[Message]) extends DeviceMessage, ActuatorMessage

  case class SubscribeAck(author: ActorRef[Message]) extends Message

  case class Unsubscribe(replyTo: ActorRef[Message]) extends DeviceMessage

  case class UnsubscribeAck(author: ActorRef[Message]) extends Message

  case class MessageWithReply[T](message: T, replyTo: ActorRef[Message], args: T*) extends Message

  case class MessageWithoutReply[T](message: T, args: T*) extends ActuatorMessage

  case class Approved() extends ActuatorMessage

  case class Denied() extends ActuatorMessage

  case class Timeout() extends Message

  case class SetActuatorRef(actuator: ActorRef[Message]) extends Message

  case class Stop() extends Message

  case class ForceStateChange[T](transition: T) extends ActuatorMessage
