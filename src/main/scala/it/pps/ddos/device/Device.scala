package it.pps.ddos.device

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.{Message, Status, SubscribeAck, UnsubscribeAck}
import it.pps.ddos.device.actuator.Actuator
import it.pps.ddos.device.sensor.{Sensor, SensorActor}
import it.pps.ddos.utils.DataType
import it.pps.ddos.utils.GivenDataType.AnyDataType
import it.pps.ddos.device.DeviceProtocol.{Message, Status}
import it.pps.ddos.grouping.tagging.Taggable

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration
/*
* Abstract definition of device
* */
trait Device[T](val id: String, protected var destinations: List[ActorRef[Message]]) extends Taggable:
  protected var status: Option[T] = None
  def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    if requester == selfId then status match
      case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
      case None =>
  def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = ()
  def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit = ()
  def behavior(): Behavior[Message]

/*
* Abstract definition of device modules
* */
trait Timer(val duration: FiniteDuration):
  self: Device[_] =>
  override def behavior(): Behavior[Message] = this match
    case sensor: Sensor[Any, Any] => SensorActor(sensor).behaviorWithTimer(duration)
    case actuator: Actuator[_] => actuator.behaviorWithTimer(duration)

trait Public[T]:
  self: Device[T] =>
  override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
    case Some(value) =>
      for (actor <- destinations)
        actor ! Status[T](selfId, value)
        println(actor)
    case None =>

  override def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit =
    if (!(destinations contains toAdd))
      destinations = toAdd :: destinations
      toAdd ! SubscribeAck(selfId)

  override def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit =
    destinations = destinations.filter(_ != toRemove)
    toRemove ! UnsubscribeAck(selfId)