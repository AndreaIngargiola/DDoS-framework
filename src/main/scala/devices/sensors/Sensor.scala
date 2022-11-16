package devices.sensors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import devices.sensors.TimedSensorActor.Message.SelfMessage

import scala.concurrent.duration.FiniteDuration

/*
* Define logic sensors
* */
trait Sensor[A] {
  var internalStatus: A = _

  def processingFunction: A => A
  def setStatus(phyInput: A): Unit =
    internalStatus = processingFunction(phyInput)
  def sendMessage(msg: GetStatus): Unit
}

trait BasicSensor[A] {
  self: Sensor[A] =>
  override def processingFunction: A => A = x => x
}

trait ProcessedDataSensor[A,B](processFun: B => A):
  self: Sensor[A] =>
  override def processingFunction: B => A = processFun

/*
* Actor of a basic sensor
* */
object SensorActor {
  enum Command:
    case GetStatus() extends Command
    case SetStatus[A](phyInput: A) extends Command

  def apply[A](sensor: Sensor[A]): Behavior[Command] =
    Behaviors.receiveMessage { message =>
      message match
        case GetStatus() =>
          println("Get message received. Getting status.. ")
          println("Sensor status: " + sensor.internalStatus)
          Behaviors.same
        case SetStatus(_) =>
          println("Set message received. Setting status.. ")
          sensor.setStatus(_)
          Behaviors.same
    }
}

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

/*
* Mixin example
* */
// class TemperatureSensor extends Sensor[Double] with BasicSensor[Double]