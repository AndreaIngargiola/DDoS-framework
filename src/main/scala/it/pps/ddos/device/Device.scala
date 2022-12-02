package it.pps.ddos.device

import akka.actor.typed.ActorRef
import it.pps.ddos.device.DeviceProtocol.{Message, Status}

import scala.collection.immutable.List

trait Device[T](protected var destinations: List[ActorRef[Message]]){
  protected var status: Option[T] = None
  def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit =
    if requester == selfId then status match
      case Some(value) => for (actor <- destinations) actor ! Status[T](selfId, value)
      case None =>

  def subscribe(selfId: ActorRef[Message], toAdd: ActorRef[Message]): Unit = ()

  def unsubscribe(selfId: ActorRef[Message], toRemove: ActorRef[Message]): Unit = ()
}
