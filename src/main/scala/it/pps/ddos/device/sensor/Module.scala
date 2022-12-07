package it.pps.ddos.device.sensor

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.device.Device
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.DeviceProtocol.*

import scala.concurrent.duration.FiniteDuration

trait Condition[A, B](condition: (A | B) => Boolean, replyTo: ActorRef[Message]):
  self: Sensor[A, B] =>
  override def update(selfId: ActorRef[Message], physicalInput: B): Unit =
    self.status = Option(preProcess(physicalInput))
    condition(self.status.get) match
      case true => replyTo ! Status[A](selfId, self.status.get)
      case _ =>

trait Public[A]:
  self: Device[A] =>
  override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    status match
      case Some(value) =>
        for (actor <- destinations)
          actor ! Status[A](selfId, value)
          println(actor)
      case None =>

  override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit =
    if (!(destinations contains toAdd))
      destinations = toAdd :: destinations;
      toAdd ! SubscribeAck(selfId)

  override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit =
    destinations = destinations.filter(_ != toRemove)
    toRemove ! UnsubscribeAck(selfId)

trait Timer(val duration: FiniteDuration):
  self: Device[_] =>




