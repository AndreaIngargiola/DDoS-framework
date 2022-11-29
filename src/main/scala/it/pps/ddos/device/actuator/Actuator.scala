package it.pps.ddos.device.actuator

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem

import scala.annotation.targetName
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import it.pps.ddos.device.sensor.SensorProtocol.*
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceBehavior

object Actuator:
    def apply[T](fsm: FSM[T]): Actuator[T] = new Actuator[T](fsm)


class Actuator[T](val FSM: FSM[T], var destinations: ActorRef[Message]*) extends Device[String](destinations.toList):
    private var currentState: State[T] = FSM.getInitialState
    this.status = Some(currentState.name)
    private var pendingState: Option[State[T]] = None
    println(s"Initial state ${FSM.getInitialState.name}")

    private def getBehavior(): Behavior[Message] = Behaviors.setup[Message] { context =>
      val _utilityActor: ActorRef[Message] = spawnUtilityActor(context)
      if (currentState.isInstanceOf[LateInit]) _utilityActor ! SetActuatorRef(context.self)
      Behaviors.receiveMessagePartial(basicActuatorBehavior(_utilityActor, context).orElse(DeviceBehavior.getBasicBehavior(this, context)))
    }

    private def basicActuatorBehavior(_utilityActor: ActorRef[Message], context: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] = {
      case MessageWithoutReply(msg: T, args: Seq[T]) =>
        messageWithoutReply(msg, _utilityActor, context, args)
        Behaviors.same
      case Approved() =>
        _utilityActor = approved(_utilityActor, context)
        Behaviors.same
      case Denied() =>
        denied()
        Behaviors.same
      case ForceStateChange(transition: T) =>
        _utilityActor = forceStateChange(_utilityActor, context, transition)
        Behaviors.same
    }

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