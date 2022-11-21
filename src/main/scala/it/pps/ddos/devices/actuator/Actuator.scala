package main.scala.it.pps.ddos.devices.actuators

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem
import it.pps.ddos.devices.actuator.{Approved, Denied, LateInit, Message, MessageWithReply, MessageWithoutReply, SetActuatorRef, State}

import scala.annotation.targetName
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object Actuator {
    def apply[T](fsm: FSM[T]) = new Actuator(fsm).actuatorBehavior()
}

class Actuator[T](val FSM: FSM[T]):
    private var currentState: State[T] = FSM.getInitialState
    private var pendingState: Option[State[T]] = None

    private def actuatorBehavior(): Behavior[Message[T]] = Behaviors.setup[Message[T]](context =>
        var utilityActor: ActorRef[Message[T]] = context.spawn(currentState.getBehavior, "utilityActor")
        Behaviors.receiveMessage { message =>
            if(currentState.isInstanceOf[LateInit]) utilityActor ! SetActuatorRef(context.self)
            message match
                case MessageWithoutReply(msg) =>
                    FSM.map(currentState, msg) match
                        case state =>
                            pendingState = Some(state)
                            implicit val timeout: akka.util.Timeout = Duration.create(10, "seconds")
                            implicit val system: ActorSystem[Nothing] = context.system
                            utilityActor ? (replyTo => MessageWithReply(msg, replyTo)) //TODO handle future
                        case null =>
                            println("No action found for this message")
                    Behaviors.same
                case Approved() =>
                    if (pendingState.isDefined)
                        currentState = pendingState.get
                        pendingState = None
                        context.stop(utilityActor)
                        utilityActor = ActorSystem(currentState.getBehavior, "utilityActor")
                    else
                        println("No pending state")
                    Behaviors.same
                case Denied() =>
                    pendingState = None
                    Behaviors.same
        })


//object main extends App:
//    val stateB = State()
//    val stateC = State()
//    val stateA = State() -- "msg1".getClass -> stateB -- "msg2".getClass -> stateC
//    stateB -- "msg3".getClass -> stateA
//    stateC -- "msg4".getClass -> stateA
//    val actuator = Actuator(stateA)


