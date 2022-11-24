package it.pps.ddos.device.actuator

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

import scala.annotation.targetName
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object Actuator {
    def apply[T](fsm: FSM[T]): Behavior[Message[T]] = new Actuator(fsm).actuatorBehavior()
}

class Actuator[T](val FSM: FSM[T]):
    private var currentState: State[T] = FSM.getInitialState
    private var pendingState: Option[State[T]] = None
    println(s"Initial state ${FSM.getInitialState.name}")

    private def actuatorBehavior(): Behavior[Message[T]] = Behaviors.setup[Message[T]](context =>
        var _utilityActor: ActorRef[Message[T]] = spawnUtilityActor(context)
        Behaviors.receiveMessage { message =>
            if(currentState.isInstanceOf[LateInit]) _utilityActor ! SetActuatorRef(context.self)
            message match
                case MessageWithoutReply(msg, args*) => messageWithoutReply(msg, _utilityActor, context, args)
                case Approved() => _utilityActor = approved(_utilityActor, context)
                case Denied() => denied()
                case GetState(replyTo) => getState(replyTo)
                case ForceStateChange(transition) => forceStateChange(transition)
            Behaviors.same
        })

    private def approved(utilityActor: ActorRef[Message[T]], context: ActorContext[Message[T]]): ActorRef[Message[T]] =
        if (pendingState.isDefined)
            currentState = pendingState.get
            pendingState = None
            utilityActor ! Stop()
            spawnUtilityActor(context)
        else
            println("No pending state")
            utilityActor

    private def denied(): Unit =
        pendingState = None

    //noinspection AccessorLikeMethodIsUnit
    private def getState(replyTo: ActorRef[Message[T]]): Unit =
        replyTo ! TellState(currentState.name)

    private def messageWithoutReply(msg: T, utilityActor: ActorRef[Message[T]], context: ActorContext[Message[T]], args: Seq[T]): Behavior[Message[T]] =
        if (FSM.map.contains((currentState, msg)) && !currentState.isInstanceOf[LateInit])
            FSM.map((currentState, msg)) match
                case state =>
                    println(s"Transition from ${currentState.name} to ${state.name}")
                    pendingState = Some(state)
                    utilityActor ! MessageWithReply(msg, context.self, args*)
                case null =>
        else println("No action found for this message")
        Behaviors.same

    private def forceStateChange(transition: T): Unit =
        currentState = FSM.map((currentState, transition))

    private def spawnUtilityActor(context: ActorContext[Message[T]]) = context.spawn(currentState.getBehavior, s"utilityActor-${java.util.UUID.randomUUID.toString}")