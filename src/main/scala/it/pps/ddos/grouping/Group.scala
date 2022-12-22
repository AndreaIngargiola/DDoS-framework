package it.pps.ddos.grouping

import scala.collection.immutable.Map
import scala.collection.immutable.List
import akka.actor.typed.ActorRef
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.sensor.Public

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}

type Actor = ActorRef[Message]
type ActorList = List[ActorRef[Message]]

abstract class Group[I, O](id: String, private val sources: ActorList, destinations: ActorList)
  extends Device[O](id, destinations) with Public[O] :
  protected var data: Map[Actor, List[I]] = Map.empty

  def getSources(): ActorList = sources

  def insert(author: Actor, newValues: List[I]): Unit = data = data + (author -> newValues)

  def reset(): Unit = data = Map.empty

  def compute(signature: Actor): Unit

  override def behavior(): Behavior[Message] = Behaviors.unhandled

  def copy(): Group[I,O]

class ReduceGroup[I, O](id: String, sources: ActorList, destinations: ActorList, val f: (O, I) => O, val neutralElem: O)
  extends Group[I, O](id, sources, destinations) :
  override def compute(signature: Actor): Unit =
    status = Option(data.values.flatten.toList.foldLeft(neutralElem)(f))

  override def copy(): ReduceGroup[I, O] = new ReduceGroup(id, sources, destinations, f, neutralElem)

private trait MultipleOutputs[O]:
  self: Device[List[O]] =>
  override def propagate(selfId: Actor, requester: Actor): Unit = status match
    case Some(value) => for (actor <- destinations) actor ! Statuses[O](selfId, value)
    case None =>

class MapGroup[I, O](id: String, sources: ActorList, destinations: ActorList, val f: I => O)
  extends Group[I, List[O]](id, sources, destinations) with MultipleOutputs[O] :
  override def compute(signature: Actor): Unit =
    status = Option(
      for {
        list <- data.values.toList
        elem <- list
      } yield f(elem)
    )

  override def copy(): MapGroup[I,O] = new MapGroup(id, sources, destinations, f)