package it.pps.ddos.device.sensor

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import it.pps.ddos.device.sensor.SensorProtocol.{Message, PropagateStatus, Status, UpdateStatus}
import it.pps.ddos.device.sensor.{BasicSensor, Sensor, SensorActor}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

class SensorsTest extends AnyFlatSpec:

  "Given a Public BasicSensor[T]" should "be able to send and update his T-type Status " in testPublicBasicSensorSendCorrect()
  it should "not be able to update his Status with a value that doesn't match the T-type" in testPublicBasicSensorStatusWrong()

  "Given a Public ProcessedDataSensor[A,B]" should " be able to process data and also send/update his Status" in testPublicProcessedDataSensorSendCorrect()
  it should "not be able to process a non B-type Status in the update() function" in testPublicProcessedDataSensorStatusWrong()

  val testKit = ActorTestKit()

  private def sendMsg(sensorActor: ActorRef[Message],testProbe: TestProbe[Message]): Unit =
    sensorActor ! PropagateStatus(testProbe.ref)
    Thread.sleep(800)

  ///BASIC SENSOR TESTS
  val testProbeBasic: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorBasic = new BasicSensor[String](List(testProbeBasic.ref)) with Public[String]
  val sensorBasicActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorBasic))

  private def testPublicBasicSensorSendCorrect(): Unit =
    //test empty msg
    sendMsg(sensorBasicActor,testProbeBasic)
    testProbeBasic.expectNoMessage()
    //test non-empty msg
    sensorBasicActor ! UpdateStatus("test")
    sendMsg(sensorBasicActor,testProbeBasic)
    testProbeBasic.expectMessage(Status(sensorBasicActor.ref, "test"))

  private def testPublicBasicSensorStatusWrong(): Unit =
    //test updating with wrong type(need to be String for working)
    assertTypeError("sensor.update(5)")


  ///PROCESSED DATA SENSOR TESTS
  val testProbeProcessed: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorProcessed = new ProcessedDataSensor[String,Int](List(testProbeProcessed.ref), x=>x.toString) with Public[String]
  val sensorProcessedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessed))

  private def testPublicProcessedDataSensorSendCorrect(): Unit =
    //test empty msg
    sendMsg(sensorProcessedActor,testProbeProcessed)
    testProbeProcessed.expectNoMessage()
    //test non-empty msg but Int it's converted in String
    sensorProcessedActor ! UpdateStatus(5)
    sendMsg(sensorProcessedActor,testProbeProcessed)
    testProbeProcessed.expectMessage(Status(sensorProcessedActor.ref, "5"))

  private def testPublicProcessedDataSensorStatusWrong(): Unit =
    //test updating with wrong type(need to be Int for working)
    assertTypeError("sensor.update(0.1)")

