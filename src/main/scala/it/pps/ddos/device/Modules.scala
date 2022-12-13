package it.pps.ddos.device

import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.DeviceProtocol.{Message, Status, SubscribeAck, UnsubscribeAck}
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

trait Public[T]:
  self: Device[T] =>
  override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    status match
      case Some(value) =>
        for (actor <- destinations)
          actor ! Status[T](selfId, value)
          println(actor)
      case None =>

  override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit =
    if (!(destinations contains toAdd))
      destinations = toAdd :: destinations;
      toAdd ! SubscribeAck(selfId)

  override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit =
    destinations = destinations.filter(_ != toRemove)
    toRemove ! UnsubscribeAck(selfId)
