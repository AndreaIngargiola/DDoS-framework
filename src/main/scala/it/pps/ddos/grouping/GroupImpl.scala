package it.pps.ddos.grouping

import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.Statuses

import scala.collection.immutable.List

class ReduceGroup[I, O](id: String, sources: ActorList, destinations: ActorList, val f: (O, I) => O, val neutralElem: O)
  extends Group[I, O](id, sources, destinations) :
  override def compute(): Unit =
    status = Option(data.values.flatten.toList.foldLeft(neutralElem)(f))

  override def copy(): ReduceGroup[I, O] = new ReduceGroup(id, sources, destinations, f, neutralElem)

  override def hashCode(): Int =
    id.hashCode() + sources.hashCode() + destinations.hashCode() + f.hashCode() + neutralElem.hashCode()

private trait MultipleOutputs[O]:
  self: Device[List[O]] =>
  override def propagate(selfId: Actor, requester: Actor): Unit = status match
    case Some(value) => for (actor <- destinations) actor ! Statuses[O](selfId, value)
    case None =>

class MapGroup[I, O](id: String, sources: ActorList, destinations: ActorList, val f: I => O)
  extends Group[I, List[O]](id, sources, destinations) with MultipleOutputs[O] :
  override def compute(): Unit =
    status = Option(
      for {
        list <- data.values.toList
        elem <- list
      } yield f(elem)
    )

  override def copy(): MapGroup[I,O] = new MapGroup(id, sources, destinations, f)

  override def hashCode(): Int =
    id.hashCode() + sources.hashCode() + destinations.hashCode() + f.hashCode()
