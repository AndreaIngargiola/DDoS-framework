package it.pps.ddos.device.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, PropagateStatus, SensorMessage, Subscribe, Unsubscribe, UpdateStatus}
import it.pps.ddos.device.DeviceBehavior
import it.pps.ddos.device.DeviceBehavior.Tick
import it.pps.ddos.utils.DataType

import scala.concurrent.duration._

/*
* Actor of a basic sensor and timed sensor
* */
object SensorActor:
  def apply[I: DataType, O: DataType](sensor: Sensor[I, O]): SensorActor[I, O] = new SensorActor(sensor)

/**
 * Actor of a basic sensor and timed sensor
 *
 * @param sensor sensor to be used
 * @tparam I type of input
 * @tparam O type of output
 */
class SensorActor[I: DataType, O: DataType](val sensor: Sensor[I, O]):
  private case object TimedSensorKey
  /**
   * Behavior of the actor
   * @param ctx context of the actor
   * @return behavior of the actor
   */
  private def getBasicSensorBehavior(ctx: ActorContext[DeviceMessage]): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case UpdateStatus[I](value) =>
      sensor.update(ctx.self, value)
      Behaviors.same
  
  /**
   * Behavior of the actor
   * @param duration every how much time the sensor send a message
   * @return behavior of the actor with Timer
   */
  def behaviorWithTimer(duration: FiniteDuration): Behavior[DeviceMessage] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timer =>
        timer.startTimerWithFixedDelay(TimedSensorKey, Tick, duration)
        Behaviors.receiveMessagePartial(getBasicSensorBehavior(context)
          .orElse(DeviceBehavior.getBasicBehavior(sensor, context))
          .orElse(DeviceBehavior.getTimedBehavior(sensor, context)))
      }
    }

  def behavior(): Behavior[DeviceMessage] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial(getBasicSensorBehavior(context)
      .orElse(DeviceBehavior.getBasicBehavior(sensor, context)))
  }