package it.pps.ddos.grouping

import scala.collection.immutable.Map
import scala.collection.immutable.List
import it.pps.ddos.device.sensor.Public
import akka.actor.typed.ActorRef
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.*

abstract class Group[I,O](val sources: List[ActorRef[Message]], destinations: List[ActorRef[Message]]) extends Device[O](destinations) with Public[O]:
  var data: Map[ActorRef[Message], List[I]] = Map.empty
  def insert(author: ActorRef[Message], newValues: List[I]): Unit = data = data + (author -> newValues)
  def reset(): Unit = data = Map.empty;
  def compute(signature: ActorRef[Message]): Unit

class MapGroup[I,O](sources: List[ActorRef[Message]],
                    destinations: List[ActorRef[Message]],
                    val f: I => O) extends Group[I, List[O]](sources, destinations):
  override def compute(signature: ActorRef[Message]): Unit =
    status = Option.empty
    for (i <- data.values.flatten) yield status = Option(status.getOrElse(List.empty) ++ List(f(i)))