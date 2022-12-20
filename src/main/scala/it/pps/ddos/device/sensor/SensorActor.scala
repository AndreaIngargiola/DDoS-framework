package it.pps.ddos.device.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Subscribe, Unsubscribe, UpdateStatus}
import it.pps.ddos.device.DeviceBehavior
import it.pps.ddos.device.DeviceBehavior.Tick
import it.pps.ddos.utils.DataType

import scala.concurrent.duration.FiniteDuration

/*
* Actor of a basic sensor and timed sensor
* */
object SensorActor:
  def apply[I: DataType, O: DataType](sensor: Sensor[I, O]): SensorActor[I, O] = new SensorActor(sensor)

private class SensorActor[I: DataType, O: DataType](val sensor: Sensor[I, O]):
  private case object TimedSensorKey

  private def getBasicSensorBehavior(ctx: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] =
    case UpdateStatus[I](value) =>
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