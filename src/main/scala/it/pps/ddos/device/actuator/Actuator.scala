package it.pps.ddos.device.actuator

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

import scala.annotation.targetName
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import it.pps.ddos.device.sensor.SensorProtocol._
import it.pps.ddos.device.Device

object Actuator {
    def apply[T](fsm: FSM[T]): Actuator[T] = new Actuator[T](fsm)
}

class Actuator[T](val FSM: FSM[T], var destinations: ActorRef[Message]*) extends Device[String](destinations.toList):
    private var currentState: State[T] = FSM.getInitialState
    this.status = Some(currentState.name)
    private var pendingState: Option[State[T]] = None
    println(s"Initial state ${FSM.getInitialState.name}")

    private def getBehavior(): Behavior[Message] = Behaviors.setup[Message](context =>
        var _utilityActor: ActorRef[Message] = spawnUtilityActor(context)
        Behaviors.receiveMessage { message =>
            if(currentState.isInstanceOf[LateInit]) _utilityActor ! SetActuatorRef(context.self)
            message match
                case MessageWithoutReply(msg: T, args: Seq[T]) => messageWithoutReply(msg, _utilityActor, context, args)
                case Approved() => _utilityActor = approved(_utilityActor, context)
                case Denied() => denied()
                case ForceStateChange(transition: T) => _utilityActor = forceStateChange(_utilityActor, context, transition)
                case Subscribe(replyTo: ActorRef[Message]) => this.subscribe(context.self, replyTo)
                case Unsubscribe(replyTo: ActorRef[Message]) => this.unsubscribe(context.self, replyTo)
            Behaviors.same
        })

    private def approved(utilityActor: ActorRef[Message], context: ActorContext[Message]): ActorRef[Message] =
        if (pendingState.isDefined)
            currentState = pendingState.get
            this.status = Some(currentState.name)
            propagate(context.self, context.self)
            pendingState = None
            utilityActor ! Stop()
            spawnUtilityActor(context)
        else
            println("No pending state")
            utilityActor

    private def denied(): Unit =
        pendingState = None

    private def messageWithoutReply(msg: T, utilityActor: ActorRef[Message], context: ActorContext[Message], args: Seq[T]): Behavior[Message] =
        if (FSM.map.contains((currentState, msg)) && !currentState.isInstanceOf[LateInit])
            FSM.map((currentState, msg)) match
                case state =>
                    println(s"Transition from ${currentState.name} to ${state.name}")
                    pendingState = Some(state)
                    utilityActor ! MessageWithReply(msg, context.self, args*)
                case null =>
        else println("No action found for this message")
        Behaviors.same

    private def forceStateChange(utilityActor: ActorRef[Message], context: ActorContext[Message], transition: T): ActorRef[Message] =
        currentState = FSM.map((currentState, transition))
        utilityActor ! Stop()
        propagate(context.self, context.self)
        spawnUtilityActor(context)

    private def spawnUtilityActor(context: ActorContext[Message]) = context.spawn(currentState.getBehavior, s"utilityActor-${java.util.UUID.randomUUID.toString}")