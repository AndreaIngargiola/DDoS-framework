package it.pps.ddos.device.actuator

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import it.pps.ddos.device.DeviceProtocol.*

import scala.concurrent.duration.FiniteDuration

private case object TimerKey

type Sender[T] = () => (T, Seq[T])

class TimedState[T] (name: String, timer: FiniteDuration, senderFunction: Sender[T]) extends State[T](name), LateInit:

    var actuator: Option[ActorRef[Message]] = None
    lazy val behavior: Behavior[Message] = Behaviors.withTimers(timers => idle(timers, timer))

    private def idle(timers: TimerScheduler[Message],
                     after: FiniteDuration): Behavior[Message] = Behaviors.receiveMessage[Message]{ msg =>
            timers.startSingleTimer(TimerKey, Timeout(), after)
            msg match
                case SetActuatorRef(ref) =>
                    actuator = Some(ref)
            active(timers, after)
        }

    private def active(timers: TimerScheduler[Message], after: FiniteDuration): Behavior[Message] =
        Behaviors.receiveMessage[Message] { msg =>
            msg match
                case Timeout() =>
                    val (msg, args) = senderFunction() //TODO Handle args?
                    if(actuator.isDefined)
                        actuator.get ! ForceStateChange(msg)
                    idle(timers, after)
                case _ => Behaviors.same
        }

    override def getBehavior: Behavior[Message] = behavior

    override def copy(): State[T] = TimedState(name, timer, senderFunction)