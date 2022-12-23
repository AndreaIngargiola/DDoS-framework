package it.pps.ddos.grouping
import scala.collection.immutable.List
import scala.util.Try
import akka.actor.typed.ActorRef
import it.pps.ddos.device.DeviceProtocol.Message

trait Taggable:
  private var tags: List[Tag] = List.empty

  private def addTag(t: Tag): Unit =
    t.getTags().contains(this) match
      case false => tags = tags ++ List(t)
      case true => throw new IllegalArgumentException("circular tag nesting detected")

  def ##(newTags: Tag*): Try[Unit] = Try { for (t <- newTags) yield addTag(t) }

  def getTags(): List[Tag] = tags

abstract class Tag(val id: String) extends Taggable:
  def <--(toTag: Taggable*): Unit = for (taggable <- toTag) yield taggable ## this
  def generateGroup(sources: ActorList): Group[_,_]

case class MapTag[I, O](override val id: String, val f: I => O, val tm: TriggerMode) extends Tag(id):
  override def generateGroup(sources: ActorList) = new MapGroup[I,O](id, sources, List.empty, f)

case class ReduceTag[I, O](override val id: String, val f: (O, I) => O, val neutralElem: O, val tm: TriggerMode) extends Tag(id):
  override def generateGroup(sources: ActorList) = new ReduceGroup[I,O](id, sources, List.empty, f, neutralElem)

object Tag:
  def apply[I,O](id: String, f: I => O, tm: TriggerMode): MapTag[I,O] = MapTag(id, f, tm)
  def apply[I,O](id: String, f: (O, I) => O, neutralElem: O, tm: TriggerMode): ReduceTag[I,O] = ReduceTag(id, f, neutralElem, tm)
