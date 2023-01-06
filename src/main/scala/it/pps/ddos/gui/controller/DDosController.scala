package it.pps.ddos.gui.controller

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.device.{Device, Public, Timer}
import it.pps.ddos.storage.tusow.Server
import it.pps.ddos.utils.GivenDataType.given
import scalafx.scene.control.ListView

import java.io.File
import java.util.concurrent.TimeUnit
import scala.::
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration

class DDosController():

  private var listOfRef: List[ActorRef[Message]] = List.empty[ActorRef[Message]]

  def start(): Unit =

    setup()
    createActorSystem()

  private def createActorSystem(): Unit =
    implicit val system: ActorSystem[Message] = ActorSystem(Behaviors.empty, "DDosGUI", getConfigFile)
    Deployer.deploy(createBasicStateSensors(system))
    updateRef()

  private def createBasicStateSensors(actorSys: ActorSystem[Message]): Graph[Device[Double]] =
    val basic1 = new BasicSensor[Double]("0", List.empty)
    val basic2 = new BasicSensor[Double]("1", List.empty)
    val basic3 = new BasicSensor[Double]("2", List.empty)
    val basic4 = new BasicSensor[Double]("3", List.empty)
    val graph = Graph[Device[Double]](
      basic1 -> basic2,
      basic1 -> basic3,
      basic2 -> basic4,
      basic3 -> basic1
    )
    graph

  private def setup(): Unit =
    Deployer.initSeedNodes()
    Deployer.addNodes(5)
  //Server.start()
  //TusowBinder.start()

  private def updateRef(): Unit =
    var buffer = ListBuffer.empty[ActorRef[Message]]
    for (ref <- Deployer.getDevicesActorRefMap.toList)
      buffer += ref._2
    listOfRef = buffer.toList
    println(listOfRef)

  def updateData(): Unit = updateRef()

  def getListOfRef: List[ActorRef[Message]] = listOfRef

  private def getConfigFile: Config =
    val configFile = getClass.getClassLoader.getResource("application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    config
