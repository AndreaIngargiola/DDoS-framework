package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.utils.DataType

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/*
* Abstract definition of sensor
* */
trait Sensor[I: DataType, O: DataType] extends Device[O]:
  def preProcess: I => O
  def update(selfId: ActorRef[Message], physicalInput: I): Unit = this.status = Option(preProcess(physicalInput))

/*
* Abstract definition of sensor modules
* */
trait Condition[I: DataType, O: DataType](condition: (I | O) => Boolean, replyTo: ActorRef[Message]):
  self: Sensor[I, O] =>
  override def update(selfId: ActorRef[Message], physicalInput: I): Unit =
    self.status = Option(preProcess(physicalInput))
    condition(self.status.get) match
      case true => replyTo ! Status[O](selfId, self.status.get)
      case _ =>

/*
* Concrete definition of sensor types
* */
class BasicSensor[O: DataType](id: String, destinations: List[ActorRef[Message]]) extends Device[O](id, destinations) with Sensor[O, O]:
  override def preProcess: O => O = x => x
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()

class ProcessedDataSensor[I: DataType, O: DataType](id: String, destinations: List[ActorRef[Message]], processFun: I => O)
  extends Device[O](id, destinations) with Sensor[I, O]:
  override def preProcess: I => O = processFun
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()