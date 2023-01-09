package it.pps.ddos.device

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, PropagateStatus, Subscribe, Unsubscribe}
import it.pps.ddos.device.Device

/**
 * Actor behavior definition of a device
 */
object DeviceBehavior:
  private[device] case object Tick extends DeviceMessage //declaration of the the private message for the timed actor

  /**
   * The basic actor behavior definition of a device
   *
   * @param device
   * @param ctx the device actor context
   * @tparam T is the device type
   * @return PartialFunction[DeviceMessage, Behavior[DeviceMessage]]
   */
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

  /**
   * The timed actor behavior definition of a device
   *
   * @param device
   * @param ctx the device actor context
   * @tparam T is the device type
   * @return PartialFunction[DeviceMessage, Behavior[DeviceMessage]]
   */
  def getTimedBehavior[T](device: Device[T], ctx: ActorContext[DeviceMessage]): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case Tick =>
      device.propagate(ctx.self, ctx.self)
      Behaviors.same
