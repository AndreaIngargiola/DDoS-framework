package it.pps.ddos.devices.sensors

import it.pps.ddos.devices.sensors.SensorProtocol.SendStatus

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}


import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A, B](val destination: ActorRef[_]):
  var status: Option[A] = Option.empty

  def preProcess: B => A
  def update(physicalInput: B) = status = Option(preProcess(physicalInput))
  def propagate(sensorID: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
    case Some(value) => destination ! Status[A](sensorID, value)
    case None() =>


trait BasicSensor[A]:
  self: Sensor[A] =>
  override def processingFunction: A => A = x => x

trait ProcessedDataSensor[A,B](processFun: B => A):
  self: Sensor[A] =>
  override def processingFunction: B => A = processFun

/*
* Actor of a basic sensor
* */
object SensorActor:

  def apply[T](sensor: Sensor[T]): Behavior[Message] =
    Behaviors.receiveMessage { message =>
      message match
        case PropagateStatus(requesterRef) =>
          println("Sending status.. ")
          sensor.propagate(context.self, requesterRef) // requesterRef is the actor that request the propagation, not the destination.
          Behaviors.same
        case UpdateStatus(value: B) =>
          sensor.update(value)
          Behaviors.same
    }
/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]