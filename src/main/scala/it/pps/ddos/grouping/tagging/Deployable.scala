package it.pps.ddos.grouping.tagging

import akka.actor.typed.Behavior
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.grouping.{BlockingGroup, Group, NonBlockingGroup}

trait Deployable[I, O](tm: TriggerMode) extends Group[I, O] :
  override def behavior(): Behavior[Message] = tm match
    case TriggerMode.BLOCKING => BlockingGroup(this)
    case TriggerMode.NONBLOCKING => NonBlockingGroup(this)

  override def hashCode(): Int = super.hashCode()

enum TriggerMode:
  case BLOCKING extends TriggerMode
  case NONBLOCKING extends TriggerMode
