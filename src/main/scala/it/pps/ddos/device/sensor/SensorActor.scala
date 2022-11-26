package it.pps.ddos.device.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.sensor.SensorProtocol.{Message, PropagateStatus, UpdateStatus}

import scala.concurrent.duration.FiniteDuration

/*
* Actor of a basic sensor and timed sensor
* */
object SensorActor:
  private case object SensorTimerKey

  /**
   * declaration of the the private message for the timed actor
   */
  private case object Tick extends Message

  def getBasicBehavior[A, B](sensor: Sensor[A, B], ctx: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] = {
    case PropagateStatus(requesterRef) =>
      println("Sending status.. ")
      sensor.propagate(ctx.self, requesterRef) // requesterRef is the actor that request the propagation, not the destination.
      Behaviors.same
    case UpdateStatus(value: B) =>
      sensor.update(ctx.self, value)
      Behaviors.same
  }

  def getTimedBehavior[A, B](sensor: Sensor[A, B]): PartialFunction[Message, Behavior[Message]] = {
    case Tick =>
      //sensor.propagate() //TODO
      Behaviors.same
  }

  def withTimer[A, B](sensor: Sensor[A, B], duration: FiniteDuration): Behavior[Message] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timer =>
        timer.startTimerAtFixedRate(SensorTimerKey, Tick, duration)
        Behaviors.receiveMessagePartial(getBasicBehavior(sensor, context).orElse(getTimedBehavior(sensor)))
      }
    }

  def apply[A, B](sensor: Sensor[A, B]): Behavior[Message] = Behaviors.setup {
    context => Behaviors.receiveMessagePartial(getBasicBehavior(sensor, context))
  }
