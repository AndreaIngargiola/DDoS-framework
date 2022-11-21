package it.pps.ddos.devices.sensors.module

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.devices.sensors.{Sensor}

import scala.concurrent.duration.FiniteDuration

trait Broadcast

trait Private[A, B]:
    self: Sensor[A, B] =>
    override def propagate(sensorID: ActorRef[Message], requester: ActorRef[Message]): Unit = requester match
        case sensorID.equals(requester) => self.propagate(sensorId, requester)
        case _ =>