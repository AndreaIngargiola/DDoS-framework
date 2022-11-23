package it.pps.ddos.devices.actuator
import akka.actor.typed.ActorRef

import scala.annotation.targetName
import scala.collection.immutable.{HashMap, ListMap}
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.devices.actuator.State

type ConditionalFunction[T] = (msg: T, args: Seq[T]) => Boolean

object ConditionalState {
    def apply[T](name: String, condFunc: ConditionalFunction[T]): ConditionalState[T] = new ConditionalState[T](name, condFunc)
}

class ConditionalState[T](name: String, condFunction: ConditionalFunction[T]) extends State[T](name):

    private val behavior: Behavior[Message[T]] = Behaviors.receiveMessage[Message[T]] { msg =>
//        println(s"State > Received message $msg")
        msg match
            case MessageWithReply(msg, replyTo, args*) =>
                println(condFunction(msg, args))
                if condFunction(msg, args) then
                    replyTo ! Approved[T]()
                else
                    replyTo ! Denied[T]()
            case _ =>
        Behaviors.same
    }

    override def getBehavior: Behavior[Message[T]] = behavior

    override def copy(): State[T] = ConditionalState(name, condFunction)