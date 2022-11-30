package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.collection.immutable.Map
import scala.collection.immutable.List
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration

trait GroupActor:
  // initial state
  def apply(group: Group[_]): Behavior[Message] =
    Behaviors.setup {
      context =>
        group.sources.foreach(_ ! Subscribe(context.self))
        connecting(group.sources, group, context)
    }

  private def connecting(sources: List[ActorRef[Message]], group: Group[_], context: ActorContext[Message]): Behavior[Message] =
    Behaviors.withTimers[Message] { timer =>
      timer.startTimerAtFixedRate(Timeout(), FiniteDuration(5, "second"))
      Behaviors.receiveMessagePartial { message =>
        (message, sources) match
          case (Timeout(), _) =>
            sources.foreach(_ ! Subscribe(context.self))
            connecting(sources, group, context)
          case (SubscribeAck(author), destinations) if destinations.length > 1 => connecting(destinations.filter(_ != author), group, context)
          case (SubscribeAck(author), destinations) if destinations.exists(_ == author) => active(group.sources, group, context)
          case _ => connecting(sources, group, context)
      }
    }

  def active(sources: List[ActorRef[Message]], group: Group[_], context: ActorContext[Message]): Behavior[Message]

object BlockingGroup extends GroupActor:
  override def active(group: Group[_], context: ActorContext[Message]): Behavior[Message] =
    Behaviors.receive[Message] { (message, context) =>
      message match
        case Status(author, value) =>
        case Statuses(author, values) =>
        case _ =>
    }

class Group[T](val sources: List[ActorRef[Message]]) {
  var values: Map[ActorRef[Message], T]

}
