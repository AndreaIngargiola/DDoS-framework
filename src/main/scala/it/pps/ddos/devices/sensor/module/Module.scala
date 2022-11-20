package it.pps.ddos.devices.sensor.module

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.devices.sensor.{Message, Sensor}

import scala.concurrent.duration.FiniteDuration

trait ConditionalModule[A](condition: Boolean, replyTo: ActorRef[A]):
    self: Sensor[A] =>
    override def setStatus(phyInput: A): Unit =
        this.internalStatus = processingFunction(phyInput)
        if condition then replyTo ! internalStatus


trait TimedModule[A](timer: TimerScheduler[Message], val duration: FiniteDuration, var status: A):
    self: Sensor[A] =>
    status = self.internalStatus
    override def setStatus(phyInput: A): Unit = self.setStatus(phyInput)
