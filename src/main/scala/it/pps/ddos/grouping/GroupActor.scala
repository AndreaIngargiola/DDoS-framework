package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import it.pps.ddos.device.DeviceBehavior
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, Subscribe, SubscribeAck, Timeout}

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration


trait GroupActor:
  // initial state
  def apply[M >: DeviceMessage](g: Group[_,_]): Behavior[M] =
    Behaviors.setup[M] {
      context =>
        g.getSources().foreach(_ ! Subscribe(context.self))
        connecting(g.getSources(), g.copy())
    }

  def connecting[M >: DeviceMessage](sources: ActorList, g: Group[_,_]): Behavior[M] =
    Behaviors.withTimers[M] { timer =>
      timer.startTimerAtFixedRate("connectingStateTimer", Timeout(), FiniteDuration(1, "second"))
      Behaviors.receivePartial { (context, message) =>
        (message, sources) match
          case (Timeout(), _) =>
            sources.foreach(_ ! Subscribe(context.self))
            Behaviors.same
          case (SubscribeAck(author), sources) if sources.length > 1 =>
            connecting(sources.filter(_ != author), g)
          case (SubscribeAck(author), sources) if sources.contains(author) =>
            timer.cancel("connectingStateTimer")
            active(g.getSources(), g, context)
          case _ =>
            Behaviors.same
      }
    }

  def active[M >: DeviceMessage](sources: ActorList, g: Group[_,_], context: ActorContext[M]): Behavior[M] =
    Behaviors.receiveMessagePartial(getTriggerBehavior(context, g, sources).orElse(DeviceBehavior.getBasicBehavior(g, context)))

  def getTriggerBehavior[I,O,M >: DeviceMessage](context: ActorContext[M],
                              g: Group[I,O],
                              sources: ActorList): PartialFunction[M, Behavior[M]]