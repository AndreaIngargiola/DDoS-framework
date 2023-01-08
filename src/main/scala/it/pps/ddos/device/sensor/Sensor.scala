package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.utils.{DataType, GivenDataType}
import it.pps.ddos.utils.GivenDataType.*

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/**
 * @tparam I type of input
 * @tparam O type of output
* Abstract definition of sensor
* */
trait Sensor[I: DataType, O: DataType] extends Device[O]:
  status = Option(DataType.defaultValue[O])
  def preProcess: I => O
  def update(selfId: ActorRef[SensorMessage], physicalInput: I): Unit = this.status = Option(preProcess(physicalInput))

/*
* Abstract definition of sensor modules
* */
trait Condition[I: DataType, O: DataType](condition: O => Boolean, replyTo: ActorRef[DeviceMessage]):
  self: Sensor[I, O] =>
  override def update(selfId: ActorRef[SensorMessage], physicalInput: I): Unit =
    self.status = Option(preProcess(physicalInput))
    if condition(self.status.get) then replyTo ! Status[O](selfId, self.status.get)

/**
 * Sensor that send his status to a List of destination
 * @param id The id of the sensor
 * @param destinations List of actors to which send the status
 * @tparam O type of the status
 */
class BasicSensor[O: DataType](id: String, destinations: List[ActorRef[DeviceMessage]]) extends Device[O](id, destinations) with Sensor[O, O]:
  override def preProcess: O => O = x => x
  override def behavior(): Behavior[DeviceMessage] = SensorActor(this).behavior()

/**
 * Sensor that send his status after the function is apply to the input
 * @param id The id of the sensor
 * @param destinations List of actors to which send the status
 * @param processFun The function to apply to the input
 * @tparam I type of input
 * @tparam O type of output
 */
class ProcessedDataSensor[I: DataType, O: DataType](id: String,
                                                    destinations: List[ActorRef[DeviceMessage]],
                                                    processFun: I => O) extends Device[O](id, destinations) with Sensor[I, O]:
  override def preProcess: I => O = processFun
  override def behavior(): Behavior[DeviceMessage] = SensorActor(this).behavior()