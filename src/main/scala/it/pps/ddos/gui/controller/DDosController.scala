package it.pps.ddos.gui.controller

import akka.actor.typed.receptionist.Receptionist.Subscribe
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.DeviceProtocol.{Message, Status}
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.device.{Device, DeviceProtocol, Public, Timer}
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
  private var listOfMsg: List[String] = List.empty[String]
  private var guiActor: Option[ActorRef[Message]] = None
  private val key = ServiceKey[Message]("DeviceService")

  def start(): Unit =
    implicit val system: ActorSystem[Message] = ActorSystem(Behaviors.empty, "ClusterSystem", ConfigFactory.load("application.conf"))
    createGUIActor()

  private def createGUIActor()(using system: ActorSystem[Message]): Unit =
    system.systemActorOf(Behaviors.setup { context =>
        context.system.receptionist ! Receptionist.Subscribe(key, context.self)
        Behaviors.receiveMessage { msg =>
          msg match
            case Status(ref, value) =>
              //println("Received " + value + " from " + ref)
              listOfMsg = String.valueOf("Received value:  " + value + " from " + ref + " at " + System.currentTimeMillis()) :: listOfMsg
              Behaviors.same
            case key.Listing(listings) =>
              for {elem <- listings} yield {
                listOfRef.contains(elem) match
                  case false =>
                    elem ! DeviceProtocol.Subscribe(context.self)
                    listOfRef = elem :: listOfRef
                  case _ =>
              }
              Behaviors.same
            case _ => Behaviors.same
        }
    },"GUIActor")
  def getMsgHistory: List[String] = listOfMsg

  def getListOfRef: List[ActorRef[Message]] = listOfRef


