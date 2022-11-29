package it.pps.ddos.device

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.sensor.Sensor
import it.pps.ddos.device.sensor.SensorProtocol.*

object DeviceBehavior:
  /**
   * declaration of the the private message for the timed actor
   */
  protected[device] case object Tick extends Message

  def getBasicBehavior[A](device: Device[A], ctx: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] = {
    case PropagateStatus(requesterRef) =>
      device.propagate(ctx.self, requesterRef) // requesterRef is the actor that request the propagation, not the destination.
      Behaviors.same
    case Subscribe(replyTo) =>
      device.subscribe(ctx.self, replyTo)
      Behaviors.same
    case Unsubscribe(replyTo) =>
      device.unsubscribe(ctx.self, replyTo)
      Behaviors.same
  }

  def getTimedBehavior[A](device: Device[A], ctx: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] = {
    case Tick =>
      device.propagate(ctx.self, ctx.self)
      Behaviors.same
  }
