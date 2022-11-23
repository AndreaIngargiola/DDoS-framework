package it.pps.ddos.devices.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration
import it.pps.ddos.devices.sensor.{Message, GetStatus, SetStatus}

/*
* Define logic sensors
* */
trait Sensor[A]:
  var internalStatus: A = _

  def processingFunction: A => A
  def setStatus(phyInput: A): Unit =
    internalStatus = processingFunction(phyInput)
  def sendMessage(msg: GetStatus): Unit


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
        case GetStatus() =>
          println("Get message received. Getting status.. ")
          println("Sensor status: " + sensor.internalStatus)
          Behaviors.same
        case SetStatus(_) =>
          println("Set message received. Setting status.. ")
          sensor.setStatus(_)
          Behaviors.same
    }
/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]