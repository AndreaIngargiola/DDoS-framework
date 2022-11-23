package it.pps.ddos.devices.sensor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.devices.sensor.SelfMessage
import it.pps.ddos.devices.sensor.module.TimedModule

private case object SensorTimerKey

object TimedSensorActor:

    def apply[A](timedSensor: TimedModule[A]): Behavior[Message] =
        Behaviors.setup { _ =>
            Behaviors.withTimers { timer =>
                timer.startTimerAtFixedRate(SensorTimerKey, SelfMessage(), timedSensor.duration)
                Behaviors.receiveMessage { message =>
                    message match
                        case SelfMessage() =>
                            timedSensor.setStatus(timedSensor.status)
                            Behaviors.same
                }
            }
        }