package it.pps.ddos.device

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.{ActuatorMessage, DeviceMessage, SensorMessage, Status, SubscribeAck, UnsubscribeAck}
import it.pps.ddos.device.actuator.Actuator
import it.pps.ddos.device.sensor.{Sensor, SensorActor}
import it.pps.ddos.utils.DataType
import it.pps.ddos.utils.GivenDataType.AnyDataType
import it.pps.ddos.grouping.tagging.Taggable

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
/*
* Abstract definition of device
* */
trait Device[T](val id: String, protected var destinations: List[ActorRef[DeviceMessage]]) extends Taggable:
  protected var status: Option[T] = None
  def propagate(selfId: ActorRef[DeviceMessage], requester: ActorRef[DeviceMessage]): Unit =
    if requester == selfId then status match
      case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
      case None =>
  def subscribe(selfId: ActorRef[DeviceMessage], toSubscribe: ActorRef[DeviceMessage]): Unit = ()
  def unsubscribe(selfId: ActorRef[DeviceMessage], toUnsubscribe: ActorRef[DeviceMessage]): Unit = ()
  def behavior(): Behavior[DeviceMessage]

/*
* Abstract definition of device modules
* */
trait Timer(val duration: FiniteDuration):
  self: Device[_] =>
  override def behavior(): Behavior[DeviceMessage] = this match
    case sensor: Sensor[Any, Any] => SensorActor(sensor).behaviorWithTimer(duration)
    case actuator: Actuator[_] => actuator.behaviorWithTimer(duration)

trait Public[T]:
  self: Device[T] =>
  override def propagate(selfId: ActorRef[DeviceMessage], requester: ActorRef[DeviceMessage]): Unit = status match
    case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
    case None =>

  override def subscribe(selfId: ActorRef[DeviceMessage], toSubscribe: ActorRef[DeviceMessage]): Unit =
    if !(destinations contains toSubscribe) then
      destinations = toSubscribe :: destinations
      toSubscribe ! SubscribeAck(selfId)

  override def unsubscribe(selfId: ActorRef[DeviceMessage], toUnsubscribe: ActorRef[DeviceMessage]): Unit =
    destinations = destinations.filter(_ != toUnsubscribe)
    toUnsubscribe ! UnsubscribeAck(selfId)