package devices.sensors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import devices.sensors.TimedSensorActor.Message.SelfMessage

import scala.concurrent.duration.FiniteDuration

trait ConditionalSensor[A](condition: Boolean, replyTo: ActorRef[A]):
  self: Sensor[A] =>
  override def setStatus(phyInput: A): Unit =
    this.internalStatus = processingFunction(phyInput)
    if condition then replyTo ! internalStatus

trait TimedSensor[A](timer: TimerScheduler[String], val duration: FiniteDuration, var status: A):
  self: Sensor[A] =>
  status = self.internalStatus
  override def setStatus(phyInput: A): Unit = self.setStatus(phyInput)

object TimedSensorActor {

  enum Message:
    case SelfMessage

  def apply[A](timedSensor: TimedSensor[A]): Behavior[Message] =
    Behaviors.setup { _ =>
      Behaviors.withTimers { timer =>
        timer.startTimerAtFixedRate(SelfMessage, timedSensor.duration)
        Behaviors.receiveMessage { message => message match
          case SelfMessage =>
            timedSensor.setStatus(timedSensor.status)
            Behaviors.same
        }
      }
    }
}