package it.pps.ddos.devices.sensor.module

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.devices.sensor.Sensor
import it.pps.ddos.devices.sensor.SensorProtocol.*

import scala.concurrent.duration.FiniteDuration

trait ConditionalModule[A](condition: Boolean, replyTo: ActorRef[A]):
    self: Sensor[A, A] =>
    override def update(phyInput: A): Unit = condition match
        case true =>
            self.status = Option(preProcess(phyInput))
            replyTo ! self.status.get
        case _ =>

trait TimedModule[A](timer: TimerScheduler[Message], val duration: FiniteDuration, var status: A):
    self: Sensor[A, A] =>
    status = self.status
    override def update(phyInput: A): Unit = self.update(phyInput)

trait Private[A, B]:
    self: Sensor[A, B] =>
    override def propagate(sensorID: ActorRef[Message], requester: ActorRef[Message]): Unit =
        if requester == sensorID then self.propagate(sensorID, requester)
    override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit
    override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit
