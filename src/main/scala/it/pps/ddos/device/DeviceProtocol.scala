package it.pps.ddos.device

import akka.actor.typed.ActorRef
import scala.collection.immutable.List

/* Definition of the message protocol shared by devices */
object DeviceProtocol:
  trait Message
  trait DeviceMessage extends Message
  trait SensorMessage extends DeviceMessage
  trait ActuatorMessage extends DeviceMessage

  abstract class Output[T](author: ActorRef[_ >: DeviceMessage], value: T) extends DeviceMessage

  case class Status[T](author: ActorRef[_ >: DeviceMessage], value: T) extends Output[T](author, value)

  case class Statuses[T](author: ActorRef[_ >: DeviceMessage], value: List[T]) extends Output[List[T]](author, value)

  case class PropagateStatus(requester: ActorRef[DeviceMessage]) extends DeviceMessage

  case class UpdateStatus[T](value: T) extends SensorMessage

  case class Subscribe(replyTo: ActorRef[DeviceMessage]) extends DeviceMessage

  case class SubscribeAck(author: ActorRef[DeviceMessage]) extends DeviceMessage

  case class Unsubscribe(replyTo: ActorRef[DeviceMessage]) extends DeviceMessage

  case class UnsubscribeAck(author: ActorRef[DeviceMessage]) extends DeviceMessage

  case class MessageWithReply[T](message: T, replyTo: ActorRef[_ >: ActuatorMessage], args: T*) extends ActuatorMessage

  case class MessageWithoutReply[T](message: T, args: T*) extends ActuatorMessage

  case class Approved() extends ActuatorMessage

  case class Denied() extends ActuatorMessage

  case class Timeout() extends ActuatorMessage

  case class SetActuatorRef(actuator: ActorRef[_ >: ActuatorMessage]) extends ActuatorMessage

  case class Stop() extends ActuatorMessage

  case class ForceStateChange[T](transition: T) extends ActuatorMessage
