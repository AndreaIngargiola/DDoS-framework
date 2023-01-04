package it.pps.ddos.device

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.{ActuatorMessage, DeviceMessage, Status, SubscribeAck, UnsubscribeAck}
import it.pps.ddos.device.actuator.Actuator
import it.pps.ddos.device.sensor.{Sensor, SensorActor}
import it.pps.ddos.utils.DataType
import it.pps.ddos.utils.GivenDataType.AnyDataType
import it.pps.ddos.grouping.tagging.Taggable

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration
/*
* Abstract definition of device
* */
trait Device[T](val id: String, protected var destinations: List[ActorRef[_ >: DeviceMessage]]) extends Taggable:
  protected var status: Option[T] = None
  def propagate[M >: DeviceMessage](selfId: ActorRef[M], requester: ActorRef[M]): Unit =
    if requester == selfId then status match
      case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
      case None =>
  def subscribe[M >: DeviceMessage](selfId: ActorRef[M], toSubscribe: ActorRef[M]): Unit = ()
  def unsubscribe[M >: DeviceMessage](selfId: ActorRef[M], toUnsubscribe: ActorRef[M]): Unit = ()
  def behavior[M >: DeviceMessage](): Behavior[M]

/*
* Abstract definition of device modules
* */
trait Timer(val duration: FiniteDuration):
  self: Device[_] =>
  override def behavior[M >: DeviceMessage](): Behavior[M] = this match
    case sensor: Sensor[Any, Any] => SensorActor(sensor).behaviorWithTimer(duration)
    case actuator: Actuator[_] => actuator.behaviorWithTimer(duration)

trait Public[T]:
  self: Device[T] =>
  override def propagate[M >: DeviceMessage](selfId: ActorRef[M], requester: ActorRef[M]): Unit = status match
    case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
    case None =>

  override def subscribe[M >: DeviceMessage](selfId: ActorRef[M], toSubscribe: ActorRef[M]): Unit =
    if !(destinations contains toSubscribe) then
      destinations = toSubscribe :: destinations
      toSubscribe ! SubscribeAck(selfId)

  override def unsubscribe[M >: DeviceMessage](selfId: ActorRef[M], toUnsubscribe: ActorRef[M]): Unit =
    destinations = destinations.filter(_ != toUnsubscribe)
    toUnsubscribe ! UnsubscribeAck(selfId)