package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.utils.MeasureType

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[I >: MeasureType, O >: MeasureType] extends Device[O]:
  def preProcess: I => O
  def update(selfId: ActorRef[Message], physicalInput: I): Unit = this.status = Option(preProcess(physicalInput))

class BasicSensor[O >: MeasureType](id: String, destinations: List[ActorRef[Message]]) extends Device[O](id, destinations) with Sensor[O, O]:
  override def preProcess: O => O = x => x
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()

class ProcessedDataSensor[I >: MeasureType, O >: MeasureType](id: String, destinations: List[ActorRef[Message]], processFun: I => O)
  extends Device[O](id, destinations) with Sensor[I, O]:
  override def preProcess: I => O = processFun
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()