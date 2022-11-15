package devices.sensors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import devices.sensors.SensorActor.Command.{GetStatus, SetStatus}
import devices.sensors.TimedSensorActor.Message.SelfMessage

import scala.concurrent.duration.FiniteDuration

trait BasicSensor[A]:
  self: Sensor[A] =>
  override def processingFunction: A => A = x => x

trait ProcessedDataSensor[A,B](processFun: B => A):
  self: Sensor[A] =>
  override def processingFunction: B => A = processFun
