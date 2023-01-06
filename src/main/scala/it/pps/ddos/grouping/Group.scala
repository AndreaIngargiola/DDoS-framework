package it.pps.ddos.grouping

import scala.collection.immutable.Map
import scala.collection.immutable.List
import akka.actor.typed.ActorRef
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.Public

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

type Actor = ActorRef[DeviceMessage]
type ActorList = List[ActorRef[DeviceMessage]]

abstract class Group[I, O](id: String, private val sources: ActorList, destinations: ActorList)
  extends Device[O](id, destinations) with Public[O] :
  protected var data: Map[Actor, List[I]] = Map.empty

  def getSources(): ActorList = sources

  def insert(author: Actor, newValues: List[I]): Unit = data = data + (author -> newValues)

  def reset(): Unit = data = Map.empty

  def compute(signature: Actor): Unit

  override def behavior(): Behavior[DeviceMessage] = Behaviors.unhandled

  def copy(): Group[I,O]

  // Defining canEqual method
  def canEqual(a: Any) = a.isInstanceOf[Group[_,_]]

  // Defining equals method with override keyword
  override def equals(that: Any): Boolean =
    that match
      case that: Group[_,_] => that.canEqual(this) &&
        this.hashCode == that.hashCode
      case _ => false

