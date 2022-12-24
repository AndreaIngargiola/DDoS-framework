package it.pps.ddos

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.actuator.{Actuator, BasicState, FSM}
import org.scalatest.flatspec.AnyFlatSpec
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.device.Device
import it.pps.ddos.device.sensor.BasicSensor

import scala.collection.immutable.ListMap

class DeployerTest extends AnyFlatSpec:
  val testKit: ActorTestKit = ActorTestKit()
  Deployer.initSeedNodes()
  Deployer.addNodes(5)

  private def createTestBasicStateSensor(): Graph[Device[Double]] =
    val testProbe = testKit.createTestProbe[Message]()
    val basic1 = new BasicSensor[Double]("1", List(testProbe.ref))
    val basic2 = new BasicSensor[Double]("1", List(testProbe.ref))
    val basic3 = new BasicSensor[Double]("1", List(testProbe.ref))
    val basic4 = new BasicSensor[Double]("1", List(testProbe.ref))
    val graph = Graph[Device[Double]](
      basic1 -> basic2,
      basic2 -> basic3,
      basic3 -> basic4,
      basic4 -> basic1
    )
    graph

  "A graph of sensors should" should "be deploy without errors" in {
    Thread.sleep(2000)
    Deployer.deploy(createTestBasicStateSensor())
  }

  private def createTestBasicStateFSM(): FSM[String] =
    val stateA = BasicState[String]("A")
    val stateB = BasicState[String]("B")
    val stateC = BasicState[String]("C")
    var fsm = FSM[String](Option.empty, Option.empty, ListMap.empty)
    fsm = stateA -- "toB" -> stateB _U stateA -- "toC" -> stateC _U stateB -- "toC" -> stateC _U stateC -- "toA" -> stateA
    fsm

  "A graph of actuator should" should "be deployed without errors" in {
    Thread.sleep(2000)
    val fsm = createTestBasicStateFSM()
    val a1 = Actuator[String]("a1", fsm)
    val a2 = Actuator[String]("a2", fsm)
    val a3 = Actuator[String]("a3", fsm)
    val a4 = Actuator[String]("a4", fsm)
    val graph = Graph[Device[String]](
      a1 -> a2,
      a1 -> a3,
      a2 -> a4,
      a3 -> a1
    )
    Deployer.deploy(graph)
  }
