package it.pps.ddos.device.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A, B](val destination: ActorRef[Status[_]]):
  var status: Option[A] = Option.empty

  def preProcess: B => A

  def update(physicalInput: B): Unit = status = Option(preProcess(physicalInput))

  def propagate(sensorID: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
    case Some(value) => destination ! Status[A](sensorID, value)
    case None =>


class BasicSensor[A](destination: ActorRef[Status[_]]) extends Sensor[A, A](destination) :
  override def preProcess: A => A = x => x

class ProcessedDataSensor[A, B](destination: ActorRef[Status[_]], processFun: B => A) extends Sensor[A, B](destination) :
  override def preProcess: B => A = processFun

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]