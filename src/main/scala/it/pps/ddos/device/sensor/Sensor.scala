package it.pps.ddos.devices.sensor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.devices.sensor.SensorProtocol.*
import scala.collection.immutable.List

import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A, B](protected var destinations: List[ActorRef[Status[_]]]):
  protected var status: Option[A] = Option.empty
  def preProcess: B => A
  def update(physicalInput: B): Unit = status = Option(preProcess(physicalInput))
  def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    if requester == selfId then status match
      case Some(value) => for(actor <- destinations) actor ! Status[A](selfId, value)
      case None =>
  def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = ()
  def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit = ()

class BasicSensor[A](destinations: List[ActorRef[Status[_]]]) extends Sensor[A, A](destinations) :
  override def preProcess: A => A = x => x

class ProcessedDataSensor[A, B](destinations: List[ActorRef[Status[_]]], processFun: B => A) extends Sensor[A, B](destinations) :
  override def preProcess: B => A = processFun

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]