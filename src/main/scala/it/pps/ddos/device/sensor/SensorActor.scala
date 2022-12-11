package it.pps.ddos.device.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.DeviceProtocol.{ Message, PropagateStatus, UpdateStatus, Subscribe, Unsubscribe }
import it.pps.ddos.device.DeviceBehavior
import it.pps.ddos.device.DeviceBehavior.Tick

import scala.concurrent.duration.FiniteDuration

/*
* Actor of a basic sensor and timed sensor
* */
object SensorActor:
  def apply[A, B](sensor: Sensor[A, B]): SensorActor[A, B] = new SensorActor(sensor)

class SensorActor[A, B](val sensor: Sensor[A, B]):
  private case object TimedSensorKey

  private def getBasicSensorBehavior(ctx: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] =
    case UpdateStatus[B](value) =>
      sensor.update(ctx.self, value)
      Behaviors.same

  def behaviorWithTimer(duration: FiniteDuration): Behavior[Message] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timer =>
        timer.startTimerAtFixedRate(TimedSensorKey, Tick, duration)
        Behaviors.receiveMessagePartial(getBasicSensorBehavior(context)
          .orElse(DeviceBehavior.getBasicBehavior(sensor, context))
          .orElse(DeviceBehavior.getTimedBehavior(sensor, context)))
      }
    }

  def behavior(): Behavior[Message] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial(getBasicSensorBehavior(context).orElse(DeviceBehavior.getBasicBehavior(sensor, context)))
  }