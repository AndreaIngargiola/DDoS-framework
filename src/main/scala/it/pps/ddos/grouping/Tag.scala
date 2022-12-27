package it.pps.ddos.grouping
import scala.collection.immutable.List
import scala.util.Try
import akka.actor.typed.ActorRef
import it.pps.ddos.device.DeviceProtocol.Message

trait Taggable:
  private var tags: List[Tag[_,_]] = List.empty

  private def addTag(t: Tag[_,_]): Unit =
    t.getTags().contains(this) match
      case false => tags = tags ++ List(t)
      case true => throw IllegalArgumentException("circular tag nesting detected")

  def ##(newTags: Tag[_,_]*): Try[Unit] = Try { for (t <- newTags) yield addTag(t) }

  def getTags(): List[Tag[_,_]] = tags

abstract class Tag[I,O](val id: String) extends Taggable:
  def <--(toTag: Taggable*): Unit = for (taggable <- toTag) yield taggable ## this
  def generateGroup(sources: ActorList): Group[I,O]

case class MapTag[I,O](override val id: String, val f: I => O, val tm: TriggerMode) extends Tag[I, List[O]](id):
  override def generateGroup(sources: ActorList): MapGroup[I,O] = new MapGroup(id, sources, List.empty, f) with Deployable[I,List[O]](tm)

case class ReduceTag[I,O](override val id: String, val f: (O, I) => O, val neutralElem: O, val tm: TriggerMode) extends Tag[I,O](id):
  override def generateGroup(sources: ActorList): ReduceGroup[I,O] = new ReduceGroup(id, sources, List.empty, f, neutralElem) with Deployable[I,List[O]](tm)

object Tag:
  def apply[I,O](id: String, f: I => O, tm: TriggerMode): MapTag[I,O] = new MapTag[I,O](id, f, tm)
  def apply[I,O](id: String, f: (O, I) => O, neutralElem: O, tm: TriggerMode): ReduceTag[I,O] = ReduceTag(id, f, neutralElem, tm)
