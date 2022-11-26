package it.pps.ddos.device.sensor

import org.scalatest.flatspec.AnyFlatSpec
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.ActorRef.ActorRefOps
import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import it.pps.ddos.device.sensor.SensorProtocol.{Message, PropagateStatus, Status, UpdateStatus}

/*
* Sensor classes for tests
* */


class SensorTest extends AnyFlatSpec:
  "A Sensor " should "be able to receive PropagateStatus and UpdateStatus messages" in testSensorActorCorrect()
  it should "not be able to receive different kinds of messages" in testSensorActorWrong()

  val testKit: ActorTestKit = ActorTestKit()
  val testValue: Double = 2.0

  private def sendPropagateStatusMessage(sensor: ActorRef[Message], ref: ActorRef[Message]) =
    sensor ! PropagateStatus(ref)
    Thread.sleep(800)

  private def testSensorActorCorrect(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensor = testKit.spawn(SensorActor(new BasicSensor[Double](testProbe.ref)))
    // test of PropagateStatus case
    sendPropagateStatusMessage(sensor, testProbe.ref)
    testProbe.expectNoMessage()
    // test of PropagateStatus case
    sensor ! UpdateStatus(testValue)
    Thread.sleep(800)
    sendPropagateStatusMessage(sensor, testProbe.ref)
    testProbe.expectMessage(Status(sensor.ref, testValue))

  private def testSensorActorWrong(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensor = testKit.spawn(SensorActor(new BasicSensor[Double](testProbe.ref)))


