package it.pps.ddos.group

import akka.actor.testkit.typed.Effect.Spawned
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.sensor.{BasicSensor, Public, Sensor, SensorActor}
import org.scalatest.flatspec.AnyFlatSpec
import it.pps.ddos.grouping.*
import it.pps.ddos.grouping.TriggerModes.*
import org.scalactic.Prettifier.default

import scala.collection.immutable.List
import scala.concurrent.duration.Duration

class GroupTest extends AnyFlatSpec:

  "A group of sensor" should "apply a user defined function to each sensor of the group and produce an aggregatd output" in testSubscription()

  val testKit: ActorTestKit = ActorTestKit()

  private def preparePublicSensor(id: String): ActorRef[Message] =
    class PublicSensor extends BasicSensor[String]() with Public[String]
    val sensor = testKit.spawn(SensorActor(new PublicSensor).behavior())
    sensor ! UpdateStatus("Status of sensor "+id)
    sensor

  private object Determinizer:
    def apply(destination: ActorRef[Message]): Behavior[Message] =
      Behaviors.setup { ctx =>
        Behaviors.receivePartial { (ctx, message) =>
          message match
            case Status[scala.collection.immutable.List[String]](author, value) =>
              destination ! Status(author, value.sorted)
              Behaviors.same
        }
      }
  private def testSubscription(): Unit =
    var sensors: List[ActorRef[Message]] = List.empty
    for i <- 1 to 3 yield sensors = sensors ++ List(preparePublicSensor(i.toString))
    val testProbe = testKit.createTestProbe[Message]()
    val determinizer = testKit.spawn(Determinizer(testProbe.ref))
    val toUppercaseActor = testKit.spawn(BlockingGroup(new MapGroup[String, String](sensors, List(determinizer), f => f.toUpperCase )))
    Thread.sleep(500)
    sensors.foreach(s => s ! PropagateStatus(testProbe.ref))
    testProbe.expectMessage(Status(toUppercaseActor.ref, List("STATUS OF SENSOR 1", "STATUS OF SENSOR 2", "STATUS OF SENSOR 3")))