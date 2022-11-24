package it.pps.ddos.devices.sensor

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.devices.sensor.SensorProtocol._

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
* Actor of a basic sensor
* */
object SensorActor:

  def apply[A, B](sensor: Sensor[A, B]): Behavior[Message] =
    Behaviors.setup { context =>
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
    }

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]