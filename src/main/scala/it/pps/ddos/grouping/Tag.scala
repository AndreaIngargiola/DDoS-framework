package it.pps.ddos.grouping
import scala.collection.immutable.List

trait Taggable:
  private var tags: List[Tag] = List.empty
  def ##(newTags: Tag*): Unit = for (tag <- newTags) tags = tags ++ List(tag)
  def getTags(): List[Tag] = tags

abstract class Tag(val id: String) extends Taggable:
  def <--(toTag: Taggable*): Unit = for (taggable <- toTag) yield taggable ## this

case class MapTag[I, O](override val id: String, val f: I => O) extends Tag(id)
case class ReduceTag[I, O](override val id: String, val f: (I,I) => O) extends Tag(id)

enum TriggerType:
  case Blocking
  case NonBlocking

object Tag:
  def apply[I,O](id: String, f: I => O, triggerType: TriggerType): MapTag[I,O] = MapTag(id, f)
  def apply[I,O](id: String, f: (O, I) => O, neutralElem: O, triggerType: TriggerType): ReduceTag[I,O] = ReduceTag(id, f)
