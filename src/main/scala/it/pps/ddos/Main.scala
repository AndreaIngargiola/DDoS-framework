package it.pps.ddos

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.{Device, Public, Timer}
import it.pps.ddos.utils.GivenDataType.given
import it.pps.ddos.device.DeviceProtocol.{DeviceMessage, Message, Status, Statuses, UpdateStatus}
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.grouping.tagging
import it.pps.ddos.grouping.tagging.TriggerMode
import it.pps.ddos.gui.view.DDosGUI
import it.pps.ddos.storage.tusow.{Server, TusowBinder}
import javafx.application.Application
import javafx.embed.swing.JFXPanel
import org.agrona.concurrent.status.StatusIndicatorReader
import scalafx.application.JFXApp3

import scala.concurrent.ExecutionContext.Implicits.global
import java.io.File
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit =
    setupDeployer()
    executeParallelTask(LaunchTusowService.main(args))
    executeParallelTask(updateDeviceStatus())
    DDosGUI.main(args)

  private def executeParallelTask[T](code: => T): Future[T] = Future(code)

  private def updateDeviceStatus(): Unit =
    val actorMap = Deployer.getDevicesActorRefMap
    for i <- 1 to 100 do
      actorMap.foreach((device, actorRef) => actorRef ! UpdateStatus((Math.random()*100).toInt))
      Thread.sleep(5000)
      println(Deployer.getDevicesActorRefMap)

  private def setupDeployer(): Unit =
    Deployer.initSeedNodes()
    Deployer.addNodes(5)
    executeParallelTask(Deployer.deploy(createSensors()))
    Thread.sleep(2000)

  private def createSensors(): Graph[Device[String]] =
    val addNumber = tagging.Tag[String, String]("tag2", List.empty, "1 " + _, TriggerMode.BLOCKING)
    val addNumber2 = tagging.Tag[String, String]("tag3", List.empty, "2 " + _, TriggerMode.BLOCKING)
    val addNumber3 = tagging.Tag[String, String]("tag4", List.empty, "3 " + _, TriggerMode.BLOCKING)
    val addNumber4 = tagging.Tag[String, String]("tag5", List.empty, "4 " + _, TriggerMode.BLOCKING)
    val basic1 = new BasicSensor[String]("0", List.empty) with Public[String] with Timer(Duration(10, "second"))
    val basic2 = new BasicSensor[String]("1", List.empty) with Public[String] with Timer(Duration(10, "second"))

    addNumber ## addNumber2
    addNumber2 ## addNumber3
    addNumber3 ## addNumber4
    addNumber4 ## addNumber
    basic1 ## addNumber
    basic2 ## addNumber2

    val graph = Graph[Device[String]](
      basic1 -> basic2,
      basic2 -> basic1,
    )
    graph

}
