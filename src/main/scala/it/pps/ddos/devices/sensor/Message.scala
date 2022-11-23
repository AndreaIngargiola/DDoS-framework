package it.pps.ddos.devices.sensor

trait Message

case class SetStatus[T](status: T) extends Message
case class GetStatus() extends Message
case class SelfMessage() extends Message