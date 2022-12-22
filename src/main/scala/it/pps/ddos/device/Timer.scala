package it.pps.ddos.device

import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.device.actuator.Actuator
import it.pps.ddos.device.sensor.{Sensor, SensorActor}

import scala.concurrent.duration.FiniteDuration

trait Timer(val duration: FiniteDuration):
  self: Device[_] =>

  override def behavior(): Behavior[Message] =
    if (this.isInstanceOf[Sensor[_, _]])
      SensorActor(this.asInstanceOf[Sensor[_, _]]).behaviorWithTimer(duration)
    else
      this.asInstanceOf[Actuator[_]].behaviorWithTimer(duration)
