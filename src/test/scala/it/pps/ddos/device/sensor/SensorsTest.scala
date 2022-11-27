package it.pps.ddos.device.sensor

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import it.pps.ddos.device.sensor.SensorProtocol.{Message, PropagateStatus, Status, UpdateStatus}
import it.pps.ddos.device.sensor.{BasicSensor, Sensor, SensorActor}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import scala.concurrent.duration.FiniteDuration

class SensorsTest extends AnyFlatSpec :

  "A Public BasicSensor[T]" should "be able to send and update his T-type Status " in testPublicBasicSensorSendCorrect()
  it should "not be able to update his Status with a value that doesn't match the T-type" in testPublicBasicSensorStatusWrong()

  "A Public ProcessedDataSensor[A,B]" should " be able to process data and also send/update his Status" in testPublicProcessedDataSensorSendCorrect()
  it should "not be able to process a non B-type value in the update() function" in testPublicProcessedDataSensorStatusWrong()

  "A Public Basic-ConditionSensor" should "be able and update his Status only if the condition is True" in testBasicConditionSensorCorrect()
  it should "not be able to update his Status if the condition is False" in testBasicConditionSensorWrong()

  "A Public ProcessedData-ConditionSensor" should "be able and update his Status only if the condition is True " +
    "and if a B-Type value is pass to the update function" in testProcessedConditionSensorCorrect()
  it should "not be able to update his Status otherwise" in testProcessedConditionSensorWrong()

  "A Public Basic-TimedSensor" should "be able and update his Status only if the time is passed" in testBasicTimedSensorCorrect()
  it should "not be able to update his Status otherwise" in testBasicTimedSensorWrong()

  "A Public ProcessedData-TimedSensor" should "be able and update his Status only if the time is passed " +
    "and if a B-Type value is pass to the update function" in testProcessedTimedSensorCorrect()
  it should "not be able to update his Status otherwise" in testProcessedTimedSensorWrong()

  val testKit = ActorTestKit()

  private def sendMsg(sensorActor: ActorRef[Message], testProbe: TestProbe[Message]): Unit =
    sensorActor ! PropagateStatus(testProbe.ref)
    Thread.sleep(800)

  ///BASIC SENSOR TESTS
  val testProbeBasic: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorBasic = new BasicSensor[String](List(testProbeBasic.ref)) with Public[String]
  val sensorBasicActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorBasic))

  private def testPublicBasicSensorSendCorrect(): Unit =
    //test empty msg
    sendMsg(sensorBasicActor, testProbeBasic)
    testProbeBasic.expectNoMessage()
    //test non-empty msg
    sensorBasicActor ! UpdateStatus("test")
    sendMsg(sensorBasicActor, testProbeBasic)
    testProbeBasic.expectMessage(Status(sensorBasicActor.ref, "test"))

  private def testPublicBasicSensorStatusWrong(): Unit =
  //test updating with wrong type(need to be String for working)
    assertTypeError("sensor.update(sensorBasicActor.ref,5)")


  ///PROCESSED DATA SENSOR TESTS
  val testProbeProcessed: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorProcessed = new ProcessedDataSensor[String, Int](List(testProbeProcessed.ref), x => x.toString) with Public[String]
  val sensorProcessedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessed))

  private def testPublicProcessedDataSensorSendCorrect(): Unit =
    //test empty msg
    sendMsg(sensorProcessedActor, testProbeProcessed)
    testProbeProcessed.expectNoMessage()
    //test non-empty msg but Int it's converted in String
    sensorProcessedActor ! UpdateStatus(5)
    sendMsg(sensorProcessedActor, testProbeProcessed)
    testProbeProcessed.expectMessage(Status(sensorProcessedActor.ref, "5"))

  private def testPublicProcessedDataSensorStatusWrong(): Unit =
  //test updating with wrong type(need to be Int for working)
    assertTypeError("sensor.update(sensorProcessedActor.ref,0.1)")

  //BASIC-CONDITION SENSOR TESTS
  val testProbeBasicCondition: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorCondition = new BasicSensor[Int](List(testProbeBasicCondition.ref)) with Public[Int] with Condition[Int, Int]((_ > 5), testProbeBasicCondition.ref)
  val sensorConditionActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorCondition))

  private def testBasicConditionSensorCorrect(): Unit =
    sensorCondition.update(sensorConditionActor.ref, 6) //updating trigger the automatic send of the current status to the testProbe
    Thread.sleep(800)
    testProbeBasicCondition.expectMessage(Status(sensorConditionActor.ref, 6))

  private def testBasicConditionSensorWrong(): Unit =
    sensorCondition.update(sensorConditionActor.ref, 0)
    Thread.sleep(800)
    testProbeBasicCondition.expectNoMessage()

  //PROCESSED DATA-CONDITION SENSOR TESTS
  val testProbeProcessedCondition: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorProcessedCondition = new ProcessedDataSensor[String, Int](List(testProbeProcessedCondition.ref), x => x.toString) with Public[String] with Condition[String, Int]((_.toString.contains("5")), testProbeProcessedCondition.ref)
  val sensorProcessedConditionActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessedCondition))

  private def testProcessedConditionSensorCorrect(): Unit =
    sensorProcessedCondition.update(sensorProcessedConditionActor.ref, 5) //updating trigger the automatic send of the current status to the testProbe
    Thread.sleep(800)
    testProbeProcessedCondition.expectMessage(Status(sensorProcessedConditionActor.ref, "5"))

  private def testProcessedConditionSensorWrong(): Unit =
    sensorProcessedCondition.update(sensorProcessedConditionActor.ref, 0)
    Thread.sleep(800)
    testProbeProcessedCondition.expectNoMessage()

  //BASIC-TIMED SENSOR TESTS

  private def testBasicTimedSensorCorrect(): Unit =
    val testProbeBasicTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorBasicTimed = new BasicSensor[Int](List(testProbeBasicTimed.ref)) with Public[Int]
    val sensorBasicTimedActor: ActorRef[Message] = testKit.spawn(SensorActor.withTimer(sensorBasicTimed, FiniteDuration(2, "second")))
    sensorBasicTimed.update(sensorBasicTimedActor.ref, 5)

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeBasicTimed.expectMessage(Status(sensorBasicTimedActor.ref, 5))
    }
    testKit.stop(sensorBasicTimedActor)

  private def testBasicTimedSensorWrong(): Unit =
    val testProbeBasicTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorBasicTimed = new BasicSensor[Int](List(testProbeBasicTimed.ref)) with Public[Int]
    val sensorBasicTimedActor: ActorRef[Message] = testKit.spawn(SensorActor.withTimer(sensorBasicTimed, FiniteDuration(2, "second")))
    assertTypeError("sensorBasicTimed.update(sensorBasicTimedActor.ref,0.1)")
    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeBasicTimed.expectNoMessage()
    }
    testKit.stop(sensorBasicTimedActor)

  //PROCESSED DATA-TIMED SENSOR TESTS
  private def testProcessedTimedSensorCorrect(): Unit =
    val testProbeProcessedTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorProcessedTimed = new ProcessedDataSensor[String, Int](List(testProbeProcessedTimed.ref), x => x.toString) with Public[String]
    val sensorProcessedTimedActor: ActorRef[Message] = testKit.spawn(SensorActor.withTimer(sensorProcessedTimed, FiniteDuration(2, "second")))

    sensorProcessedTimed.update(sensorProcessedTimedActor.ref, 2)
    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeProcessedTimed.expectMessage(Status(sensorProcessedTimedActor.ref, "2"))
    }
    testKit.stop(sensorProcessedTimedActor)

  private def testProcessedTimedSensorWrong(): Unit =
    val testProbeProcessedTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorProcessedTimed = new ProcessedDataSensor[String, Int](List(testProbeProcessedTimed.ref), x => x.toString) with Public[String]
    val sensorProcessedTimedActor: ActorRef[Message] = testKit.spawn(SensorActor.withTimer(sensorProcessedTimed, FiniteDuration(2, "second")))

    assertTypeError("sensorProcessedTime.update(sensorProcessedTimedActor.ref,0.1)")

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeProcessedTimed.expectNoMessage()
    }
    testKit.stop(sensorProcessedTimedActor)


