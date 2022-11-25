package it.pps.ddos.devices.sensor

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

trait Public[A]:
    self: Sensor[A, _] =>
        override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
            case Some(value) => for(actor <- destinations) actor ! Status[A](selfId, value)
            case None =>
        override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = destinations.contains(toAdd) match
            case false => destinations = toAdd :: destinations; toAdd ! SubscribeAck(selfId)
            case _ =>
        override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit =
            destinations = destinations.filter(_ != toRemove); toRemove ! UnsubscribeAck(selfId)
