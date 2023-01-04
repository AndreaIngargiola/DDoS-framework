package it.pps.ddos.device

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, PropagateStatus, Subscribe, Unsubscribe}
import it.pps.ddos.device.Device

object DeviceBehavior:
  /**
   * declaration of the the private message for the timed actor
   */
  private[device] case object Tick extends DeviceMessage

  def getBasicBehavior[A, M >: DeviceMessage](device: Device[A], ctx: ActorContext[M]): PartialFunction[M, Behavior[M]] =
    case PropagateStatus(requesterRef) =>
      device.propagate(ctx.self, requesterRef) // requesterRef is the actor that request the propagation, not the destination.
      Behaviors.same
    case Subscribe(replyTo) =>
      device.subscribe(ctx.self, replyTo)
      Behaviors.same
    case Unsubscribe(replyTo) =>
      device.unsubscribe(ctx.self, replyTo)
      Behaviors.same

  def getTimedBehavior[A, M >: DeviceMessage](device: Device[A], ctx: ActorContext[M]): PartialFunction[M, Behavior[M]] =
    case Tick =>
      device.propagate(ctx.self, ctx.self)
      Behaviors.same
