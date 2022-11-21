package it.pps.ddos.devices.sensors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.devices.actuators.Message
import it.pps.ddos.devices.sensors.module.TimedModule

private case object SensorTimerKey

object TimedSensorActor:

    /**
     * declaration of the the private message for the timed actor
     */
    private case object Tick extends Message

    def apply[A](timedSensor: TimedModule[A]): Behavior[Message] =
        Behaviors.setup { _ =>
            Behaviors.withTimers { timer =>
                timer.startTimerAtFixedRate(SensorTimerKey, Tick, timedSensor.duration)
                Behaviors.receiveMessage { message =>
                    message match
                        case Tick =>
                            timedSensor.setStatus(timedSensor.status)
                            Behaviors.same
                }
            }
        }