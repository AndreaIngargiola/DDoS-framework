package it.pps.ddos.devices.actuators

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration
import it.pps.ddos.devices.actuators.LateInit

private case object TimerKey

type Sender[T] = () => T

class TimedState[T] (name: String, timer: FiniteDuration, senderFunction: Sender[T]) extends State[T](name), LateInit:

    var actuator: Option[ActorRef[Message[T]]] = None
    val behavior: Behavior[Message[T]] = Behaviors.withTimers(timers => idle(timers, actuator, timer))

    private def idle(timers: TimerScheduler[Message[T]], target: Option[ActorRef[Message[T]]],
                     after: FiniteDuration): Behavior[Message[T]] = Behaviors.receiveMessage[Message[T]]{ msg =>
            timers.startSingleTimer(TimerKey, Timeout[T](), after)
            active(timers, target, after)
        }

    private def active(timers: TimerScheduler[Message[T]], target: Option[ActorRef[Message[T]]], after: FiniteDuration): Behavior[Message[T]] =
        Behaviors.receiveMessage[Message[T]] { msg =>
            msg match
                case Timeout() =>
                    if(target.isDefined) target.get ! MessageWithoutReply(senderFunction())
                    idle(timers, target, after)
                case _ => Behaviors.same
        }

    override def getBehavior: Behavior[Message[T]] = behavior

    override def setActorRef[T](ref: ActorRef[Message[T]]): Unit = actuator