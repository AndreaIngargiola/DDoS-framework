package it.pps.ddos.device.sensor

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.device.{Device, sensor}
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.actuator.Actuator
import it.pps.ddos.utils.DataType

import scala.concurrent.duration.FiniteDuration

trait Condition[I: DataType, O: DataType](condition: (I | O) => Boolean, replyTo: ActorRef[Message]):
  self: Sensor[I, O] =>
  override def update(selfId: ActorRef[Message], physicalInput: I): Unit =
    self.status = Option(preProcess(physicalInput))
    condition(self.status.get) match
      case true => replyTo ! Status[O](selfId, self.status.get)
      case _ =>
