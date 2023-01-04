package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.utils.DataType
import it.pps.ddos.utils.GivenDataType._

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/*
* Abstract definition of sensor
* */
trait Sensor[I: DataType, O: DataType] extends Device[O]:
  status = Option(summon[DataType[O]].defaultValue)
  def preProcess: I => O
  def update[M >: SensorMessage](selfId: ActorRef[M], physicalInput: I): Unit = this.status = Option(preProcess(physicalInput))

/*
* Abstract definition of sensor modules
* */
trait Condition[I: DataType, O: DataType](condition: (I | O) => Boolean, replyTo: ActorRef[_ >: DeviceMessage]):
  self: Sensor[I, O] =>
  override def update[M >: SensorMessage](selfId: ActorRef[M], physicalInput: I): Unit =
    self.status = Option(preProcess(physicalInput))
    if condition(self.status.get) then replyTo ! Status[O](selfId.asInstanceOf[ActorRef[DeviceMessage]], self.status.get)

/*
* Concrete definition of sensor types
* */
class BasicSensor[O: DataType](id: String, destinations: List[ActorRef[_ >: DeviceMessage]]) extends Device[O](id, destinations) with Sensor[O, O]:
  override def preProcess: O => O = x => x
  override def behavior[M >: DeviceMessage](): Behavior[M] = SensorActor(this).behavior()

class ProcessedDataSensor[I: DataType, O: DataType](id: String, destinations: List[ActorRef[_ >: DeviceMessage]], processFun: I => O)
  extends Device[O](id, destinations) with Sensor[I, O]:
  override def preProcess: I => O = processFun
  override def behavior[M >: DeviceMessage](): Behavior[M] = SensorActor(this).behavior()