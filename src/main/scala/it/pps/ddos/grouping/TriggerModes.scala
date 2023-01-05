package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Status, Statuses, Timeout}

import scala.collection.immutable.List


object BlockingGroup extends GroupActor:
  private case class CrossOut(source: ActorRef[Message]) extends Message

  override def getTriggerBehavior[I,O](context: ActorContext[Message],
                                       g: Group[I,O],
                                       sources: List[ActorRef[Message]]): PartialFunction[Message, Behavior[Message]] =
    case Status[I](author, value) =>
      context.self ! Statuses(author, List(value))
      Behaviors.same
    case Statuses[I](author, value) if g.getSources().contains(author) =>
      g.insert(author, value)
      context.self ! CrossOut(author)
      Behaviors.same
    case CrossOut(source) if sources.length > 1 =>
      active(sources.filter(_ != source), g, context)
    case CrossOut(source) if sources.contains(source) =>
      g.compute(); g.reset()
      context.self ! PropagateStatus(context.self)
      active(g.getSources(), g, context)

object NonBlockingGroup extends GroupActor:
  override def getTriggerBehavior[I,O](context: ActorContext[Message],
                                       g: Group[I,O],
                                     sources: List[ActorRef[Message]]): PartialFunction[Message, Behavior[Message]] =
    case Status[I](author, value) =>
      context.self ! Statuses(author, List(value))
      Behaviors.same
    case Statuses[I](author, value) if g.getSources().contains(author) =>
      g.insert(author, value); g.compute();
      context.self ! PropagateStatus(context.self)
      Behaviors.same