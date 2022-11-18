package it.pps.ddos.devices.sensors

trait Message

case class SendStatus(master: ActorRef) extends Message
case class SelfMessage() extends Message