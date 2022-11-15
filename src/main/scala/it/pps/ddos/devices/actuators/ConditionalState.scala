package it.pps.ddos.devices.actuators
import akka.actor.typed.ActorRef

import scala.annotation.targetName
import scala.collection.immutable.{HashMap, ListMap}
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.devices.actuators.State
import it.pps.ddos.devices.actuators.{Message, MessageWithReply}

type ConditionalFunction = (msg: Any) => Boolean

class ConditionalState[T](name: String, condFunction: ConditionalFunction) extends State[T](name):

    private val behavior: Behavior[Message[T]] = Behaviors.receiveMessage[Message[T]] { msg =>
        msg match
            case MessageWithReply(msg, replyTo) =>
                if condFunction(msg) then
                    replyTo ! Approved[T]()
                else 
                    replyTo ! Denied[T]()
                Behaviors.same
            case _ => Behaviors.same
    }

    override def getBehavior: Behavior[Message[T]] = behavior