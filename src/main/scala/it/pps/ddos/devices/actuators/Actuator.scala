package main.scala.it.pps.ddos.devices.actuators

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem
import it.pps.ddos.devices.actuators.{Approved, Denied, LateInit, Message, MessageWithReply, MessageWithoutReply, State}

import scala.annotation.targetName
import scala.concurrent.duration.Duration

object Actuator {
    def apply[T](fsm: FSM[T]) = new Actuator(fsm)
}
class Actuator[T](val FSM: FSM[T]):
    private var currentState: State[T] = FSM.getInitialState
    private var pendingState: Option[State[T]] = None
    var utilityActor: ActorSystem[Message[T]] = ActorSystem(currentState.getBehavior, "utilityActor")
    val actuatorActor: ActorSystem[Message[T]] = ActorSystem(actuatorBehavior(), "Actuator")

    private def actuatorBehavior(): Behavior[Message[T]] = Behaviors.setup[Message[T]](_ => Behaviors.receiveMessage { message =>
        currentState match
            case init: LateInit => init.setActorRef(actuatorActor)
            case _ =>
        message match
            case MessageWithoutReply(msg) =>
                FSM.map(currentState, msg) match
                    case state =>
                        pendingState = Some(state)
                        implicit val timeout: akka.util.Timeout = Duration.create(10, "seconds")
                        implicit val system: ActorSystem[Message[T]] = actuatorActor
                        utilityActor ? (replyTo => MessageWithReply(msg, replyTo)) //TODO handle future
                    case null =>
                        println("No action found for this message")
                Behaviors.same
            case Approved() =>
                if (pendingState.isDefined)
                    currentState = pendingState.get
                    pendingState = None
                    utilityActor.terminate()
                    utilityActor = ActorSystem(currentState.getBehavior, "utilityActor")
                else
                    println("No pending state")
                Behaviors.same
            case Denied() =>
                pendingState = None
                Behaviors.same
    })

    @targetName("tell")
    def ! (msg: Message[T]): Unit = actuatorActor ! msg


//object main extends App:
//    val stateB = State()
//    val stateC = State()
//    val stateA = State() -- "msg1".getClass -> stateB -- "msg2".getClass -> stateC
//    stateB -- "msg3".getClass -> stateA
//    stateC -- "msg4".getClass -> stateA
//    val actuator = Actuator(stateA)


