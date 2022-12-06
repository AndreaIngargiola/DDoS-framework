package it.pps.ddos

import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.actuator.{Actuator, BasicState, FSM}
import org.scalatest.flatspec.AnyFlatSpec
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.device.DeviceProtocol.Message

import scala.collection.immutable.ListMap

class DeployerTest extends AnyFlatSpec:

    private def createTestBasicStateFSM(): FSM[String] =
        val stateA = BasicState[String]("A")
        val stateB = BasicState[String]("B")
        val stateC = BasicState[String]("C")
        var fsm = FSM[String](Option.empty, Option.empty, ListMap.empty)
        fsm = stateA -- "toB" -> stateB _U stateA -- "toC" -> stateC _U stateB -- "toC" -> stateC _U stateC -- "toA" -> stateA
        fsm

    "A graph should" should "be deployed without errors" in {
        Deployer.init(1)
        Thread.sleep(2000)
        val fsm = createTestBasicStateFSM()
        val a1 = Actuator[String]("a1", fsm)
        val a2 = Actuator[String]("a2", fsm)
        val a3 = Actuator[String]("a3", fsm)
        val a4 = Actuator[String]("a4", fsm)
        val graph = Graph[Actuator[String]](
            a1 -> a2,
            a1 -> a3,
            a2 -> a4,
            a3 -> a1
        )
        Deployer.deploy(graph)
    }
