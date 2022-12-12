package it.pps.ddos.grouping

import scala.collection.immutable.Map
import scala.collection.immutable.List
import akka.actor.typed.ActorRef
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.sensor.Public

abstract class Group[I,O](val sources: List[ActorRef[Message]], destinations: List[ActorRef[Message]]) extends Device[O](destinations) with Public[O]:
  var data: Map[ActorRef[Message], List[I]] = Map.empty
  def insert(author: ActorRef[Message], newValues: List[I]): Unit = data = data + (author -> newValues)
  def reset(): Unit = data = Map.empty;
  def compute(signature: ActorRef[Message]): Unit

private trait MultipleOutputs[O]:
  self: Device[List[O]] =>
  override def propagate(selfId: ActorRef[Message], requester: ActorRef[Message]): Unit = status match
    case Some(value) => println("sending status to "+destinations(0).toString); for (actor <- destinations) actor ! Statuses[O](selfId, value)
    case None =>

class MapGroup[I,O](sources: List[ActorRef[Message]],
                    destinations: List[ActorRef[Message]],
                    val f: I => O) extends Group[I, List[O]](sources, destinations) with MultipleOutputs[O]:
  override def compute(signature: ActorRef[Message]): Unit =
    status = Option.empty
    for (i <- data.values.flatten) yield status = Option(status.getOrElse(List.empty) ++ List(f(i)))