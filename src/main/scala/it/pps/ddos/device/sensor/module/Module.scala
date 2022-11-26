package it.pps.ddos.device.sensor.module

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.concurrent.duration.FiniteDuration

trait Condition[A, B](val condition: Boolean):
    self: Sensor[A, B] =>
    override def update(sensorID: ActorRef[Message], phyInput: B): Unit = 
        self.update(sensorID, phyInput)
        condition match
        case true =>
            sensorID ! PropagateStatus(sensorID)
        case _ =>
