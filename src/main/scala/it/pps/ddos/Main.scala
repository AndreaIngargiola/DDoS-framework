package it.pps.ddos

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.{Device, Public, Timer}
import it.pps.ddos.utils.GivenDataType.given
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.gui.view.DDosGUI
import javafx.application.Application
import javafx.embed.swing.JFXPanel
import scalafx.application.JFXApp3
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.File
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object Main{

  def main(args: Array[String]): Unit =
    setup()
    executeParallelTask(Deployer.deploy(createSensors()))
    DDosGUI.main(args)

  private def executeParallelTask[T](code: => T): Future[T] = Future(code)

  private def setup(): Unit =
    Deployer.initSeedNodes()
    Deployer.addNodes(5)
  //Server.start()
  //TusowBinder.start()

  private def createSensors(): Graph[Device[Double]] =
    val basic1 = new BasicSensor[Double]("0", List.empty) with Public[Double] with Timer(Duration(1, "second"))
    val basic2 = new BasicSensor[Double]("1", List.empty) with Public[Double] with Timer(Duration(1, "second"))
    val basic3 = new BasicSensor[Double]("2", List.empty) with Public[Double] with Timer(Duration(1, "second"))
    val basic4 = new BasicSensor[Double]("3", List.empty) with Public[Double] with Timer(Duration(1, "second"))
    val graph = Graph[Device[Double]](
      basic1 -> basic2,
      basic1 -> basic3,
      basic2 -> basic4,
      basic3 -> basic1
    )
    graph

}
