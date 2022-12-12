package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Status, Statuses, Timeout}

import scala.collection.immutable.List

object TriggerModes:
  object BlockingGroup extends GroupActor:
    private case class CrossOut(source: ActorRef[Message]) extends Message

    override def getTriggerBehavior[I,O](context: ActorContext[Message],
                                         group: Group[I,O],
                                         sources: List[ActorRef[Message]]): PartialFunction[Message, Behavior[Message]] =
      case Status[I](author, value) =>
        context.self ! Statuses(author, List(value))
        Behaviors.same
      case Statuses[I](author, value) if group.sources.contains(author) =>
        group.insert(author, value)
        context.self ! CrossOut(author)
        Behaviors.same
      case CrossOut(source) if sources.length > 1 =>
        active(sources.filter(_ != source), group, context)
      case CrossOut(source) if sources.contains(source) =>
        group.compute(context.self); group.reset()
        context.self ! PropagateStatus(context.self)
        active(group.sources, group, context)

  object NonBlockingGroup extends GroupActor:
    override def getTriggerBehavior[I,O](context: ActorContext[Message],
                                       group: Group[I,O],
                                       sources: List[ActorRef[Message]]): PartialFunction[Message, Behavior[Message]] =
      case Status[I](author, value) =>
        context.self ! Statuses(author, List(value))
        Behaviors.same
      case Statuses[I](author, value) if group.sources.contains(author) =>
        group.insert(author, value)
        group.compute(context.self);
        context.self ! PropagateStatus(context.self)
        Behaviors.same

