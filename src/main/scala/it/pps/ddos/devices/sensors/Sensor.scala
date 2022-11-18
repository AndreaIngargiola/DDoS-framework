package it.pps.ddos.devices.sensors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration
import it.pps.ddos.devices.sensors.{Message, GetStatus, SetStatus}

/*
* Define logic sensors
* */
trait Sensor[A, B](val dest: ActorRef[_]):
  var selfRef: ActorRef[Command]
  var internalStatus: A = _

  def processingFunction: B => A
  def setStatus(phyInput: B): Unit =
    internalStatus = processingFunction(phyInput)
  def sendStatus(master: ActorRef): Unit =
    dest ! Status(internalStatus)


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
        case SendStatus(ref) =>
          println("Sending status.. ")
          sensor.sendStatus(ref)
          Behaviors.same
    }
/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]