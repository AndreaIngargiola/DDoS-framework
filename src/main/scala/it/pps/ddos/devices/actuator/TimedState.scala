package it.pps.ddos.devices.actuator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.duration.FiniteDuration
import it.pps.ddos.devices.actuator.LateInit

private case object TimerKey

type Sender[T] = () => (T, Seq[T])

class TimedState[T] (name: String, timer: FiniteDuration, senderFunction: Sender[T]) extends State[T](name), LateInit:

    var actuator: Option[ActorRef[Message[T]]] = None
    lazy val behavior: Behavior[Message[T]] = Behaviors.withTimers(timers => idle(timers, timer))

    private def idle(timers: TimerScheduler[Message[T]],
                     after: FiniteDuration): Behavior[Message[T]] = Behaviors.receiveMessage[Message[T]]{ msg =>
            timers.startSingleTimer(TimerKey, Timeout[T](), after)
            msg match
                case SetActuatorRef(ref) =>
                    actuator = Some(ref)
            active(timers, after)
        }

    private def active(timers: TimerScheduler[Message[T]], after: FiniteDuration): Behavior[Message[T]] =
        Behaviors.receiveMessage[Message[T]] { msg =>
            msg match
                case Timeout() =>
                    val (msg, args) = senderFunction() //TODO Handle args?
                    if(actuator.isDefined)
                        actuator.get ! ForceStateChange(msg)
                    idle(timers, after)
                case _ => Behaviors.same
        }

    override def getBehavior: Behavior[Message[T]] = behavior

    override def copy(): State[T] = TimedState(name, timer, senderFunction)