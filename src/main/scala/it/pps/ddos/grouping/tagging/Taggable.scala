package it.pps.ddos.grouping.tagging

import scala.collection.immutable.List
import scala.util.Try

trait Taggable:
  private var tags: List[Tag[_,_]] = List.empty

  private def addTag(t: Tag[_,_]): Unit =
    t.getTags().contains(this) match
      case false => tags = tags ++ List(t)
      case true => throw IllegalArgumentException("circular tag nesting detected")

  def ##(newTags: Tag[_,_]*): Try[Unit] = Try { for (t <- newTags) yield addTag(t) }

  def getTags(): List[Tag[_,_]] = tags
