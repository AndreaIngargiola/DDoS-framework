package it.pps.ddos.grouping

import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.sensor.SensorProtocol.*

import scala.collection.immutable.Map
import scala.collection.immutable.List
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration

trait GroupActor:
  // initial state
  def apply[I,O](group: Group[I,O]): Behavior[Message] =
    Behaviors.setup {
      context =>
        group.sources.foreach(_ ! Subscribe(context.self))
        connecting(group.sources, group)
    }

  def connecting[I,O](sources: List[ActorRef[Message]], group: Group[I,O]): Behavior[Message] =
    Behaviors.withTimers[Message] { timer =>
      timer.startTimerAtFixedRate(Timeout(), FiniteDuration(5, "second"))
      Behaviors.receivePartial { (context, message) =>
        (message, sources) match
          case (Timeout(), _) =>
            sources.foreach(_ ! Subscribe(context.self))
            connecting(sources, group)
          case (SubscribeAck(author), sources) if sources.length > 1 =>
            connecting(sources.filter(_ != author), group)
          case (SubscribeAck(author), sources) if sources.contains(author) =>
            active(group.sources, group)
          case _ =>
            connecting(sources, group)
      }
    }

  def active[I,O](sources: List[ActorRef[Message]], group: Group[I,O]): Behavior[Message]

object BlockingGroup extends GroupActor:
  private case class CrossOut(source: ActorRef[Message]) extends Message

  override def active[I,O](sources: List[ActorRef[Message]], group: Group[I,O]): Behavior[Message] =
    Behaviors.receive{ (context, message) =>
      message match
        case Status[I](author, value) =>
          context.self ! Status[List[I]](author, List(value))
          Behaviors.same
        case Status[List[I]](author, value) if group.sources.contains(author) =>
          group.insert(author, value)
          context.self ! CrossOut(author)
          Behaviors.same
        case CrossOut(source) if sources.length > 1 =>
          active(sources.filter(_ != source), group)
        case CrossOut(source) if sources.contains(source) =>
          group.compute
          active(group.sources, group)
        case _ => Behaviors.same
    }

trait Group[I,O](val sources: List[ActorRef[Message]]):
  var values: Map[ActorRef[Message], List[I]]
  def insert(author: ActorRef[Message], newValues: List[I]): Unit = values = values + (author -> newValues)
  def compute(signature: ActorRef[Message]): Unit

class MapGroup[I,O](val sources: List[ActorRef[Message]], val f: I => O, val dest:ActorRef[Message]) extends Group[I,O](sources):
  override def compute(signature: ActorRef[Message]): Unit =
    var listResult: List[O]
    for (i <- values.values.flatten) yield listResult = listResult + f(i)
    dest ! Status[List[O]](signature, listResult)



