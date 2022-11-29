package it.pps.ddos.device.actuator
import akka.actor.typed.ActorRef

import scala.annotation.targetName
import scala.collection.immutable.{HashMap, ListMap}
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.device.sensor.SensorProtocol._

type ConditionalFunction[T] = (msg: T, args: Seq[T]) => Boolean

object ConditionalState {
    def apply[T](name: String, condFunc: ConditionalFunction[T]): ConditionalState[T] = new ConditionalState[T](name, condFunc)
}

class ConditionalState[T](name: String, condFunction: ConditionalFunction[T]) extends State[T](name):

    private val behavior: Behavior[Message] = Behaviors.receiveMessage[Message] { msg =>
        msg match
            case MessageWithReply(msg: T, replyTo, args: Seq[T]) =>
                if condFunction(msg, args) then
                    replyTo ! Approved()
                else
                    replyTo ! Denied()
            case _ =>
        Behaviors.same
    }

    override def getBehavior: Behavior[Message] = behavior

    override def copy(): State[T] = ConditionalState(name, condFunction)