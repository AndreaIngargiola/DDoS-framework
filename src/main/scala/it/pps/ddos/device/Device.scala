package it.pps.ddos.device

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.{Message, Status}
import it.pps.ddos.grouping.tagging.Taggable

import scala.collection.immutable.List
import scala.concurrent.duration.FiniteDuration

trait Device[T](val id: String, protected var destinations: List[ActorRef[Message]]) extends Taggable:
  protected var status: Option[T] = None
  def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    if requester == selfId then status match
      case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
      case None =>
  def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = ()
  def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit = ()
  def behavior(): Behavior[Message]
