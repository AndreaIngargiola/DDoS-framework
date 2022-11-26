package it.pps.ddos.device.sensor

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.TimerScheduler
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.concurrent.duration.FiniteDuration

trait Condition[A, B](condition: A|B=>Boolean, replyTo: ActorRef[Message]):
  self: Sensor[A, B] =>
  override def update(selfId: ActorRef[Message], phyInput: B): Unit =
    self.status = Option(preProcess(phyInput))
    condition(self.status.get) match
      case true => replyTo ! Status[A](selfId, self.status.get)
      case _ =>

trait Public[A]:
  self: Sensor[A, _] =>
  override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
    case Some(value) => for (actor <- destinations) actor ! Status[A](selfId, value)
    case None =>

  override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = destinations.contains(toAdd) match
    case false => destinations = toAdd :: destinations; toAdd ! SubscribeAck(selfId)
    case _ =>

  override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit =
    destinations = destinations.filter(_ != toRemove);
    toRemove ! UnsubscribeAck(selfId)
