package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceBehavior
import it.pps.ddos.device.DeviceProtocol.{Message, Subscribe, SubscribeAck, Timeout}

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

trait GroupActor:
  // initial state
  def apply(group: Group[_,_]): Behavior[Message] =
    Behaviors.setup {
      context =>
        group.sources.foreach(_ ! Subscribe(context.self))
        connecting(group.sources, group)
    }

  def connecting(sources: List[ActorRef[Message]], group: Group[_,_]): Behavior[Message] =
    Behaviors.withTimers[Message] { timer =>
      timer.startTimerAtFixedRate(Timeout(), FiniteDuration(1, "second"))
      Behaviors.receivePartial { (context, message) =>
        (message, sources) match
          case (Timeout(), _) =>
            sources.foreach(_ ! Subscribe(context.self))
            Behaviors.same
          case (SubscribeAck(author), sources) if sources.length > 1 =>
            connecting(sources.filter(_ != author), group)
          case (SubscribeAck(author), sources) if sources.contains(author) =>
            active(group.sources, group, context)
          case _ =>
            Behaviors.same
      }
    }

  def active(sources: List[ActorRef[Message]], group: Group[_,_], context: ActorContext[Message]): Behavior[Message] =
    Behaviors.receiveMessagePartial(getTriggerBehavior(context, group, sources).orElse(DeviceBehavior.getBasicBehavior(group, context)))

  def getTriggerBehavior[I,O](context: ActorContext[Message],
                              group: Group[I,O],
                              sources: List[ActorRef[Message]]): PartialFunction[Message, Behavior[Message]]

