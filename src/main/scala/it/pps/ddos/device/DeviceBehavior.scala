package it.pps.ddos.device

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, PropagateStatus, Subscribe, Unsubscribe}
import it.pps.ddos.device.Device

object DeviceBehavior:
  /**
   * declaration of the the private message for the timed actor
   */
  private[device] case object Tick extends DeviceMessage

  def getBasicBehavior[T](device: Device[T], ctx: ActorContext[DeviceMessage]): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case PropagateStatus(requesterRef: ActorRef[DeviceMessage]) =>
      device.propagate(ctx.self, requesterRef) // requesterRef is the actor that request the propagation, not the destination.
      Behaviors.same
    case Subscribe(replyTo: ActorRef[DeviceMessage]) =>
      device.subscribe(ctx.self, replyTo)
      Behaviors.same
    case Unsubscribe(replyTo: ActorRef[DeviceMessage]) =>
      device.unsubscribe(ctx.self, replyTo)
      Behaviors.same

  def getTimedBehavior[T](device: Device[T], ctx: ActorContext[DeviceMessage]): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case Tick =>
      device.propagate(ctx.self, ctx.self)
      Behaviors.same
