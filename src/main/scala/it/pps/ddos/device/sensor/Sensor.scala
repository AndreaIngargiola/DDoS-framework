package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A, B] extends Device[A]:
  def preProcess: B => A
  def update(selfId: ActorRef[Message], physicalInput: B): Unit = this.status = Option(preProcess(physicalInput))

class BasicSensor[A](id: String, destinations: List[ActorRef[Message]]) extends Device[A](id, destinations) with Sensor[A, A]:
  override def preProcess: A => A = x => x
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()

class ProcessedDataSensor[A, B](id: String, destinations: List[ActorRef[Message]], processFun: B => A)
  extends Device[A](id, destinations) with Sensor[A, B]:
  override def preProcess: B => A = processFun
  override def behavior(): Behavior[Message] = SensorActor(this).behavior()