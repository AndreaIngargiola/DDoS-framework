package it.pps.ddos.group

import akka.actor.testkit.typed.Effect.Spawned
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import it.pps.ddos.device.DeviceProtocol.*
import it.pps.ddos.device.sensor.{BasicSensor, Public, Sensor, SensorActor}
import org.scalatest.flatspec.AnyFlatSpec
import it.pps.ddos.grouping.*
import org.scalactic.Prettifier.default

import scala.collection.immutable.List
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Failure
import scala.util.Try

class TagTest extends AnyFlatSpec:
  "A Tag" should "register itself in any object that extends the Taggable trait" in testBasicTag()
  it should "be taggable itself, but cannot be used to create a circular tagging" in testNestedTag()
  it should "be syntactic sugar to generate a group in a simpler way" in testGroupEquality()
  it should "be applied to every taggable object passed in the <-- operator" in testInverseMarking()

  val testKit: ActorTestKit = ActorTestKit()
  class PublicSensor(id: String) extends BasicSensor[String](id, List.empty) with Public[String]

  private def preparePublicSensor(id: String): ActorRef[Message] =
    val sensor = testKit.spawn(SensorActor(new PublicSensor(id)).behavior())
    sensor ! UpdateStatus("Status of sensor " + id)
    sensor

  private def prepareDevicesList(lenght: Int): List[ActorRef[Message]] =
    var sensors: List[ActorRef[Message]] = List.empty
    for i <- 1 to lenght yield sensors = sensors ++ List(preparePublicSensor(i.toString))
    sensors

  private var sensors: List[ActorRef[Message]] = prepareDevicesList(3)
  private var testProbe = testKit.createTestProbe[Message]()
  private var determinizer = testKit.spawn(Determinizer(testProbe.ref))

  private def resetVariables(): Unit =
    sensors = prepareDevicesList(3)
    testProbe = testKit.createTestProbe[Message]()
    determinizer = testKit.spawn(Determinizer(testProbe.ref))

  private object Determinizer:
    def apply(destination: ActorRef[Message]): Behavior[Message] =
      Behaviors.setup { ctx =>
        Behaviors.receivePartial { (ctx, message) =>
          message match
            case Statuses[String](author, value) =>
              destination ! Status(author, value.sorted)
              Behaviors.same
        }
      }

  private def testBasicTag(): Unit =
    val testTag = Tag[String, String]("test", s => s, TriggerMode.BLOCKING)
    case class Probe() extends Taggable
    val p = Probe()
    p ## testTag
    assert(p.getTags() == List(testTag))

  private def testNestedTag(): Unit =
    val tag1 =  Tag[String, String]("1", s => s, TriggerMode.BLOCKING)
    val tag2 =  Tag[String, String]("2", s => s, TriggerMode.BLOCKING)
    tag1 ## tag2
    assert(tag1.getTags() == List(tag2))
    val thrown = intercept[IllegalArgumentException] {
      val ex = tag2 ## tag1
      if ex.isFailure then throw ex.failed.get
    }

  private def testGroupEquality(): Unit =
    val f: String => String = s => s
    val tag1: MapTag[String, String] =  Tag[String, String]("1", f, TriggerMode.BLOCKING)
    val generatedGroup: MapGroup[String, String] = tag1.generateGroup(sensors)
    val normalGroup: MapGroup[String, String] = new MapGroup[String, String]("1", sensors, List.empty, f)
    assert(generatedGroup.equals(normalGroup))

  private def testInverseMarking(): Unit =
    resetVariables()
    val tag1 = Tag[String, String]("1", s => s, TriggerMode.BLOCKING)
    val sensorA = new PublicSensor("A")
    val sensorB = new PublicSensor("B")
    val tagC = Tag[Int, String]("C", i => i.toString, TriggerMode.NONBLOCKING)
    tag1 <--(sensorA, sensorB, tagC)
    assert(sensorA.getTags() == List(tag1) && sensorB.getTags() == List(tag1) && tagC.getTags() == List(tag1))

