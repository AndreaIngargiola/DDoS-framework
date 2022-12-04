package it.pps.ddos.device.sensor

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, TestProbe}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import com.typesafe.config.ConfigFactory
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Status, Subscribe, SubscribeAck, Unsubscribe, UnsubscribeAck, UpdateStatus}
import it.pps.ddos.device.sensor.{BasicSensor, Sensor, SensorActor}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import scala.concurrent.duration.FiniteDuration

class SensorsTest extends AnyFlatSpec:

  /*
  * SensorActor and timed SensorActor Tests
  * */
  "A SensorActor " should "be able to receive PropagateStatus, UpdateStatus, Subscribe and Unsubscribe messages" in testSensorActorReceiveMessage()
  "A timed SensorActor " should "be able to receive the same types of Message as SensorActor" in testTimedSensorActorReceiveMessage()
  "A timed SensorActor " should "be able to send a Status message at fixed rate" in testTimedSensorActorSendMessageAtFixedRate()

  /*
  * Sensor testing with a basic mixin
  * */
  "A BasicSensor with the Condition module" should "be able to update and send its own status when the condition is verified" in testBasicSensorActorWithConditionModule()
  "A BasicSensor with the Public module" should "be able to propagate its status in broadcast" in testBasicSensorActorBroadcastWithPublicModule()
  "A BasicSensor with the Public module" should "be able to add destinations in its own list and then to send back an ack" in testBasicSensorActorSubscribeWithPublicModule()
  "A BasicSensor with the Public module" should "be able to remove destinations from its own list and then to send back an ack" in testBasicSensorActorUnsubscribeWithPublicModule()
  "A BasicSensor with the Timer module" should "be able to have the behavior of a timed SensorActor" in testBasicSensorActorWithTimerModule()
  "A ProcessedDataSensor with the Condition module" should
    "be able to both process the data and update and send its own status when the condition is verified" in testProcessedDataSensorActorWithConditionModule()
  "A ProcessedDataSensor with the Public module" should
    "be able to propagate its status in broadcast" in testProcessedDataSensorActorBroadcastWithPublicModule()
  "A ProcessedDataSensor with the Public module" should
    "be able to add destinations in its own list and then to send back an ack" in testProcessedDataSensorActorSubscribeWithPublicModule()
  "A ProcessedDataSensor with the Public module" should
    "be able to remove destinations from its own list and then to send back an ack" in testProcessedDataSensorActorUnsubscribeWithPublicModule()
  "A ProcessedDataSensor with the Timer module" should
    "be able to have the behavior of a timed SensorActor" in testProcessedDataSensorActorWithTimerModule()

  /*
    * Sensor testing with a complete mixin
    * */
  "A BasicSensor with the Condition, Public and Timer modules" should "be able to:" +
    "update and send its own status when the condition is verified;" +
    "propagate its status in broadcast;" +
    "add destinations in its own list and then to send back an ack;" +
    "remove destinations from its own list and then to send back an ack;" +
    "have the behavior of a timed SensorActor." in testBasicSensorActorWithAllModules()

  "A ProcessedDataSensor with the Condition, Public and Timer modules" should "be able to:" +
    "update and send its own status when the condition is verified;" +
    "propagate its status in broadcast;" +
    "add destinations in its own list and then to send back an ack;" +
    "remove destinations from its own list and then to send back an ack;" +
    "have the behavior of a timed SensorActor." in testProcessedDataSensorWithAllModules()

  /*
  *
  * */
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

  private def sendPropagateStatusMessage(sensor: ActorRef[Message]): Unit =
    sensor ! PropagateStatus(sensor)

  private def sendUpdateStatusMessage[A](sensor: ActorRef[Message], value: A): Unit =
    sensor ! UpdateStatus(value)
    Thread.sleep(800)

  private def sendSubscribeMessage(sensor: ActorRef[Message]): Unit =
    sensor ! Subscribe(sensor)
    Thread.sleep(800)

  private def sendUnsubscribeMessage(sensor: ActorRef[Message]): Unit =
    sensor ! Unsubscribe(sensor)
    Thread.sleep(800)

  private def testBasicSensorActorWithAllModules(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val interval: FiniteDuration = FiniteDuration(2, "seconds")
    val exampleClass = new BasicSensor[String](List.empty) with Condition[String, String](_ contains "test", testProbe.ref) with Public[String] with Timer(interval)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, "test") // this value sets true the condition
    // when the condition is verified it's expected a Status message
    testProbe.expectMessage(Status(sensor, "test"))

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectNoMessage()

      // sending the Subscribe message, so it's expected the SubscribeAck message
      sensor ! Subscribe(testProbe.ref)
      testProbe.expectMessage(SubscribeAck(sensor))
      // sending the Propagate message after the Subscribe one, so it's expected a Status message
      sensor ! PropagateStatus(sensor)
      testProbe.expectMessage(Status(sensor, "test"))
      // sending the Unsubscribe message, so it's expected UnsubscribeAck message
      sensor ! Unsubscribe(testProbe.ref)
      testProbe.expectMessage(UnsubscribeAck(sensor))
    }

    testKit.stop(sensor)

  private def testProcessedDataSensorWithAllModules(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorToSubscribe = testKit.createTestProbe[Message]()
    val interval: FiniteDuration = FiniteDuration(2, "seconds")
    val exampleClass = new ProcessedDataSensor[Int, Int](List(testProbe.ref), _ + 20) with Condition[Int, Int](_ > 10, testProbe.ref) with Public[Int] with Timer(interval)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set '20'
    sendUpdateStatusMessage(sensor, 20)
    testProbe.expectMessage(Status(sensor, 40))

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectMessage(Status(sensor, 40))

      // sending the Unsubscribe message, so it's expected UnsubscribeAck message
      sensor ! Unsubscribe(testProbe.ref)
      testProbe.expectMessage(UnsubscribeAck(sensor))
      // sending the Subscribe message, so it's expected the SubscribeAck message
      sensor ! Subscribe(testProbe.ref)
      testProbe.expectMessage(SubscribeAck(sensor))
      // sending the Propagate message after the Subscribe one, so it's expected a Status message
      sensor ! PropagateStatus(sensor)
      testProbe.expectMessage(Status(sensor, 40))
    }

    testKit.stop(sensor)

  private def testBasicSensorActorWithConditionModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val destinations: List[ActorRef[Message]] = List(testProbe.ref)
    val exampleClass = new BasicSensor[String](destinations) with Condition[String, String](_ contains "test" , destinations.head)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, "test") // this value sets true the condition
    // when the condition is verified it's expected a Status message
    testProbe.expectMessage(Status(sensor, "test"))

  private def testBasicSensorActorBroadcastWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new BasicSensor[String](List.empty) with Public[String]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, "test")
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))
    // sending the Propagate message after the Subscribe one, so it's expected a Status message
    sensor ! PropagateStatus(sensor)
    Thread.sleep(800)
    testProbe.expectMessage(Status(sensor, "test"))

  private def testBasicSensorActorSubscribeWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new BasicSensor[String](List.empty) with Public[String]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, "test")
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))

  private def testBasicSensorActorUnsubscribeWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new BasicSensor[String](List.empty) with Public[String]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, "test")
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))
    // sending the Unsubscribe message, so it's expected UnsubscribeAck message
    sensor ! Unsubscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(UnsubscribeAck(sensor))

  private def testBasicSensorActorWithTimerModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val interval: FiniteDuration = FiniteDuration(2, "seconds")
    val exampleClass = new BasicSensor[String](List.empty) with Timer(interval)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorBehavior)

    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectNoMessage()
    // sending the Propagate message, so it's expected a Status message
    sensor ! PropagateStatus(sensor)

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectNoMessage()
    }
    // sending the Unsubscribe message, so it's expected UnsubscribeAck message
    sensor ! Unsubscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectNoMessage()

    testKit.stop(sensor)

  private def testProcessedDataSensorActorWithConditionModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new ProcessedDataSensor[Int, Int](List(testProbe.ref), _ + 20) with Condition[Int, Int](x => x.isInstanceOf[Int], testProbe.ref)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set '20'
    sendUpdateStatusMessage(sensor, 20)
    // then it's expected a Status message
    testProbe.expectMessage(Status(sensor, 40))

  private def testProcessedDataSensorActorBroadcastWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new ProcessedDataSensor[Int, Int](List.empty, _ + 20) with Public[Int]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, 20)
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))
    // sending the Propagate message after the Subscribe one, so it's expected a Status message
    sensor ! PropagateStatus(sensor)
    Thread.sleep(800)
    testProbe.expectMessage(Status(sensor, 40))

  private def testProcessedDataSensorActorSubscribeWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new ProcessedDataSensor[Int, Int](List.empty, _ + 20) with Public[Int]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, 20)
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))

  private def testProcessedDataSensorActorUnsubscribeWithPublicModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val exampleClass = new ProcessedDataSensor[Int, Int](List.empty, _ + 20) with Public[Int]
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behavior()
    val sensor = testKit.spawn(sensorBehavior)

    // status update: the status is going to be set "test"
    sendUpdateStatusMessage(sensor, 20)
    testProbe.expectNoMessage()
    // sending the Subscribe message, so it's expected the SubscribeAck message
    sensor ! Subscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(SubscribeAck(sensor))
    // sending the Unsubscribe message, so it's expected UnsubscribeAck message
    sensor ! Unsubscribe(testProbe.ref)
    Thread.sleep(800)
    testProbe.expectMessage(UnsubscribeAck(sensor))


  private def testProcessedDataSensorActorWithTimerModule(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val destinations: List[ActorRef[Message]] = List(testProbe.ref)
    val interval: FiniteDuration = FiniteDuration(2, "seconds")
    val exampleClass = new ProcessedDataSensor[Int, Int](destinations, _ + 20) with Timer(interval)
    val sensorBehavior: Behavior[Message] = SensorActor(exampleClass).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorBehavior)

    sendUpdateStatusMessage(sensor, 20)
    testProbe.expectNoMessage()

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectMessage(Status(sensor, 40))

      // sending the Subscribe message, so it's expected the SubscribeAck message
      sensor ! Subscribe(testProbe.ref)
      testProbe.expectNoMessage()

      // sending the Unsubscribe message, so it's expected UnsubscribeAck message
      sensor ! Unsubscribe(testProbe.ref)
      testProbe.expectNoMessage()
    }

    testKit.stop(sensor)


  private def testPropagateStatusWithTimedSensorActor(interval: FiniteDuration): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorActor)

    // test of the PropagateStatus case
    sendPropagateStatusMessage(sensor)

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectNoMessage()
    }
    testKit.stop(sensor)

  private def testUpdateStatusWithTimedSensorActor(interval: FiniteDuration): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorActor)

    // test of the UpdateStatus case
    sendUpdateStatusMessage(sensor, 0.22)
    sendPropagateStatusMessage(sensor)

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectMessage(Status(sensor, 0.22))
    }
    testKit.stop(sensor)

  private def testSubscribeWithTimedSensorActor(interval: FiniteDuration): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorActor)

    // test of the Subscribe case
    sendSubscribeMessage(sensor)

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectNoMessage()
    }
    testKit.stop(sensor)

  private def testUnsubscribeWithTimedSensorActor(interval: FiniteDuration): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorActor)

    // test of the Unsubscribe case
    sendUnsubscribeMessage(sensor)

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectNoMessage()
    }
    testKit.stop(sensor)

  // SensorActor Tests
  private def testSensorActorReceiveMessage(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behavior()
    val sensor = testKit.spawn(sensorActor)

    // test of the PropagateStatus case
    sendPropagateStatusMessage(sensor)
    testProbe.expectNoMessage()

    // test of the UpdateStatus case
    sendUpdateStatusMessage(sensor, 0.22)
    sendPropagateStatusMessage(sensor)
    testProbe.expectMessage(Status(sensor, 0.22))

    // test of the Subscribe case
    sendSubscribeMessage(sensor)
    testProbe.expectNoMessage()

    // test of the Unsubscribe case
    sendUnsubscribeMessage(sensor)
    testProbe.expectNoMessage()


  // Timed SensorActor Tests
  def testTimedSensorActorReceiveMessage(): Unit =
    val interval: FiniteDuration = FiniteDuration(2, "seconds")

    testPropagateStatusWithTimedSensorActor(interval)
    testUpdateStatusWithTimedSensorActor(interval)
    testSubscribeWithTimedSensorActor(interval)
    testUnsubscribeWithTimedSensorActor(interval)

  def testTimedSensorActorSendMessageAtFixedRate(): Unit =
    val testProbe = testKit.createTestProbe[Message]()
    val interval: FiniteDuration = FiniteDuration(2, "seconds")
    val sensorActor: Behavior[Message] = SensorActor(new BasicSensor[Double](List(testProbe.ref))).behaviorWithTimer(interval)
    val sensor = testKit.spawn(sensorActor)

    sendUpdateStatusMessage(sensor, "test")

    for (_ <- 1 to 3) {
      Thread.sleep(interval.toMillis)
      testProbe.expectMessage(Status(sensor, "test"))
    }
    testKit.stop(sensor)


  ///BASIC SENSOR TESTS
  val testProbeBasic: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorBasic = new BasicSensor[String](List(testProbeBasic.ref)) //with Public[String]
  val sensorBasicActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorBasic).behavior())

  private def testPublicBasicSensorSendCorrect(): Unit =
    //test empty msg
    sendPropagateStatusMessage(sensorBasicActor)
    testProbeBasic.expectNoMessage()
    //test non-empty msg
    sendUpdateStatusMessage(sensorBasicActor, "test")
    sendPropagateStatusMessage(sensorBasicActor)
    testProbeBasic.expectMessage(Status(sensorBasicActor, "test"))

  private def testPublicBasicSensorStatusWrong(): Unit =
    //test updating with wrong type(need to be String for working)
    assertTypeError("sensor.update(sensorBasicActor, 5)")


  ///PROCESSED DATA SENSOR TESTS
  val testProbeProcessed: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorProcessed = new ProcessedDataSensor[String, Int](List(testProbeProcessed.ref), x => x.toString) with Public[String]
  val sensorProcessedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessed).behavior())

  private def testPublicProcessedDataSensorSendCorrect(): Unit =
    //test empty msg
    sendPropagateStatusMessage(sensorProcessedActor)
    testProbeProcessed.expectNoMessage()
    //test non-empty msg but Int it's converted in String
    sendUpdateStatusMessage(sensorProcessedActor, 5)
    sendPropagateStatusMessage(sensorProcessedActor)
    testProbeProcessed.expectMessage(Status(sensorProcessedActor, "5"))

  private def testPublicProcessedDataSensorStatusWrong(): Unit =
    //test updating with wrong type(need to be Int for working)
    assertTypeError("sensor.update(sensorProcessedActor, 0.1)")

  //BASIC-CONDITION SENSOR TESTS
  val testProbeBasicCondition: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorCondition = new BasicSensor[Int](List(testProbeBasicCondition.ref)) with Public[Int] with Condition[Int, Int](_ > 5, testProbeBasicCondition.ref)
  val sensorConditionActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorCondition).behavior())

  private def testBasicConditionSensorCorrect(): Unit =
    sensorCondition.update(sensorConditionActor, 6) //updating trigger the automatic send of the current status to the testProbe
    Thread.sleep(800)
    testProbeBasicCondition.expectMessage(Status(sensorConditionActor, 6))

  private def testBasicConditionSensorWrong(): Unit =
    sensorCondition.update(sensorConditionActor, 0)
    Thread.sleep(800)
    testProbeBasicCondition.expectNoMessage()

  //PROCESSED DATA-CONDITION SENSOR TESTS
  val testProbeProcessedCondition: TestProbe[Message] = testKit.createTestProbe[Message]()
  val sensorProcessedCondition = new ProcessedDataSensor[String, Int](List(testProbeProcessedCondition.ref), x => x.toString) with Public[String] with Condition[String, Int]((_.toString.contains("5")), testProbeProcessedCondition.ref)
  val sensorProcessedConditionActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessedCondition).behavior())

  private def testProcessedConditionSensorCorrect(): Unit =
    sensorProcessedCondition.update(sensorProcessedConditionActor, 5) //updating trigger the automatic send of the current status to the testProbe
    Thread.sleep(800)
    testProbeProcessedCondition.expectMessage(Status(sensorProcessedConditionActor, "5"))

  private def testProcessedConditionSensorWrong(): Unit =
    sensorProcessedCondition.update(sensorProcessedConditionActor, 0)
    Thread.sleep(800)
    testProbeProcessedCondition.expectNoMessage()

  //BASIC-TIMED SENSOR TESTS

  private def testBasicTimedSensorCorrect(): Unit =
    val testProbeBasicTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorBasicTimed = new BasicSensor[Int](List(testProbeBasicTimed.ref)) with Public[Int]
    val sensorBasicTimedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorBasicTimed).behaviorWithTimer(FiniteDuration(2, "second")))

    sensorBasicTimed.update(sensorBasicTimedActor, 5)

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeBasicTimed.expectMessage(Status(sensorBasicTimedActor, 5))
    }
    testKit.stop(sensorBasicTimedActor)

  private def testBasicTimedSensorWrong(): Unit =
    val testProbeBasicTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorBasicTimed = new BasicSensor[Int](List(testProbeBasicTimed.ref)) with Public[Int]
    val sensorBasicTimedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorBasicTimed).behaviorWithTimer(FiniteDuration(2, "second")))

    assertTypeError("sensorBasicTimed.update(sensorBasicTimedActor, 0.1)")

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeBasicTimed.expectNoMessage()
    }
    testKit.stop(sensorBasicTimedActor)

  //PROCESSED DATA-TIMED SENSOR TESTS
  private def testProcessedTimedSensorCorrect(): Unit =
    val testProbeProcessedTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorProcessedTimed = new ProcessedDataSensor[String, Int](List(testProbeProcessedTimed.ref), x => x.toString) with Public[String]
    val sensorProcessedTimedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessedTimed).behaviorWithTimer(FiniteDuration(2, "second")))

    sensorProcessedTimed.update(sensorProcessedTimedActor, 2)

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeProcessedTimed.expectMessage(Status(sensorProcessedTimedActor, "2"))
    }
    testKit.stop(sensorProcessedTimedActor)

  private def testProcessedTimedSensorWrong(): Unit =
    val testProbeProcessedTimed: TestProbe[Message] = testKit.createTestProbe[Message]()
    val sensorProcessedTimed = new ProcessedDataSensor[String, Int](List(testProbeProcessedTimed.ref), x => x.toString) with Public[String]
    val sensorProcessedTimedActor: ActorRef[Message] = testKit.spawn(SensorActor(sensorProcessedTimed).behaviorWithTimer(FiniteDuration(2, "second")))

    assertTypeError("sensorProcessedTime.update(sensorProcessedTimedActor, 0.1)")

    for (_ <- 1 to 3) {
      Thread.sleep(2000)
      testProbeProcessedTimed.expectNoMessage()
    }
    testKit.stop(sensorProcessedTimedActor)


