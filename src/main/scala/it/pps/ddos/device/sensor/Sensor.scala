package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.Device
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A, B](protected var destinations: List[ActorRef[Status[_]]]) extends Device[A]:

  def preProcess: B => A

  def update(selfId: ActorRef[Message], physicalInput: B): Unit = this.status = Option(preProcess(physicalInput))

class BasicSensor[A](destinations: ActorRef[Status[_]]*) extends Sensor[A, A](destinations.toList) :
  override def preProcess: A => A = x => x

class ProcessedDataSensor[A, B](destinations: List[ActorRef[Status[_]]], processFun: B => A) extends Sensor[A, B](destinations) :
  override def preProcess: B => A = processFun

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]