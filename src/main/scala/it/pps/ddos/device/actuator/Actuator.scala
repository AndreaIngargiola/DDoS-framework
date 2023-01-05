package it.pps.ddos.device.actuator

import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import it.pps.ddos.device.DeviceBehavior.Tick
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.actuator.Actuator.TimedActuatorKey
import it.pps.ddos.device.{Device, DeviceBehavior}

import scala.annotation.targetName
import scala.collection.immutable.ArraySeq
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.postfixOps

object Actuator:
    private case object TimedActuatorKey
    def apply[T](id: String, fsm: FSM[T]): Actuator[T] = new Actuator[T](id, fsm)

class Actuator[T](id: String, val FSM: FSM[T], destinations: ActorRef[Message]*) extends Device[String](id, destinations.toList):
    private var currentState: State[T] = FSM.getInitialState
    this.status = Some(currentState.name)
    private var pendingState: Option[State[T]] = None
    private var utilityActor: Option[ActorRef[Message]] = None

    /**
     * @return returns the behavior to spawn the actuator
     */
    override def behavior(): Behavior[Message] = Behaviors.setup[Message] { context =>
      utilityActor = Some(spawnUtilityActor(context))
      if (currentState.isInstanceOf[LateInit] && utilityActor.isDefined) utilityActor.get ! SetActuatorRef(context.self)
      Behaviors.receiveMessagePartial(basicActuatorBehavior(context).orElse(DeviceBehavior.getBasicBehavior(this, context)))
    }

    def behaviorWithTimer(duration: FiniteDuration): Behavior[Message] =
        Behaviors.setup { context =>
          Behaviors.withTimers { timer =>
            timer.startTimerAtFixedRate(TimedActuatorKey, Tick, duration)
            Behaviors.receiveMessagePartial(basicActuatorBehavior(context)
            .orElse(DeviceBehavior.getBasicBehavior(this, context))
            .orElse(DeviceBehavior.getTimedBehavior(this, context)))
          }
        }

    private def basicActuatorBehavior(context: ActorContext[Message]): PartialFunction[Message, Behavior[Message]] = message =>
        message match
            case MessageWithoutReply(msg: T, args: _*) =>
                messageWithoutReply(msg, context, args.asInstanceOf[Seq[T]])
                Behaviors.same
            case Approved() =>
                utilityActor = approved(context)
                Behaviors.same
            case Denied() =>
                denied()
                Behaviors.same
            case ForceStateChange(transition: T) =>
                utilityActor = Some(forceStateChange(context, transition))
                Behaviors.same
            case Subscribe(requester: ActorRef[Message]) =>
                subscribe(context.self, requester)
                Behaviors.same
            case PropagateStatus(requester: ActorRef[Message]) =>
                propagate(context.self, requester)
                Behaviors.same

    private def approved(context: ActorContext[Message]): Option[ActorRef[Message]] =
        if (pendingState.isDefined)
            currentState = pendingState.get
            this.status = Some(currentState.name)
            propagate(context.self, context.self)
            pendingState = None
            utilityActor.get ! Stop()
            Some(spawnUtilityActor(context))
        else
            println("No pending state")
            utilityActor

    private def denied(): Unit =
        pendingState = None

    private def messageWithoutReply(msg: T, context: ActorContext[Message], args: Seq[T]): Behavior[Message] =
        if (FSM.map.contains((currentState, msg)) && !currentState.isInstanceOf[LateInit])
            FSM.map((currentState, msg)) match
                case state =>
                    pendingState = Some(state)
                    utilityActor.get ! MessageWithReply(msg, context.self, args*)
                case null =>
        else println("No action found for this message")
        Behaviors.same

    private def forceStateChange(context: ActorContext[Message], transition: T): ActorRef[Message] =
        currentState = FSM.map((currentState, transition))
        this.status = Some(currentState.name)
        utilityActor.get ! Stop()
        propagate(context.self, context.self)
        spawnUtilityActor(context)

    private def spawnUtilityActor(context: ActorContext[Message]) = context.spawn(currentState.getBehavior, s"utilityActor-${java.util.UUID.randomUUID.toString}")