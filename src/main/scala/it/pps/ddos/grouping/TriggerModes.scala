package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, PropagateStatus, Status, Statuses, Timeout}

import scala.collection.immutable.List


object BlockingGroup extends GroupActor:
  private case class CrossOut(source: ActorRef[_ <: Message]) extends DeviceMessage

  override def getTriggerBehavior[I,O](context: ActorContext[DeviceMessage],
                                       g: Group[I,O],
                                       sources: ActorList): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case Status[I](author, value) =>
      context.self ! Statuses(author, List(value))
      Behaviors.same
    case Statuses[I](author: Actor, value) if g.getSources().contains(author) =>
      g.insert(author, value)
      context.self ! CrossOut(author)
      Behaviors.same
    case CrossOut(source) if sources.length > 1 =>
      active(sources.filter(_ != source), g, context)
    case CrossOut(source) if sources.contains(source) =>
      g.compute(context.self); g.reset()
      context.self ! PropagateStatus(context.self)
      active(g.getSources(), g, context)

object NonBlockingGroup extends GroupActor:
  override def getTriggerBehavior[I,O](context: ActorContext[DeviceMessage],
                                       g: Group[I,O],
                                     sources: ActorList): PartialFunction[DeviceMessage, Behavior[DeviceMessage]] =
    case Status[I](author, value) =>
      context.self ! Statuses(author, List(value))
      Behaviors.same
    case Statuses[I](author: Actor, value) if g.getSources().contains(author) =>
      g.insert(author, value); g.compute(context.self);
      context.self ! PropagateStatus(context.self)
      Behaviors.same