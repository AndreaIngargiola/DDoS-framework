package it.pps.ddos.storage.tusow

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.{Device, Public, Timer}
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Status, Subscribe}
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.storage.tusow.TusowBinder.TUSOW_SYSTEM_NAME
import it.pps.ddos.storage.tusow.client.Client
import it.unibo.coordination.tusow.grpc.{IOResponse, ReadOrTakeRequest, Template, Tuple, TupleSpaceID, TupleSpaceType, TusowServiceClient, WriteRequest}
import it.pps.ddos.utils.GivenDataType.given

import java.io.File
import java.util.concurrent.TimeUnit
import scala.::
import scala.collection.mutable.*
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration.Duration

object TusowBinder:
  private final val TUSOW_SYSTEM_NAME = "ddos-tusow-storage"

  @main
  def main(): Unit =

    setup()

    implicit val sys: ActorSystem[Message] = ActorSystem(Behaviors.empty, "ClusterSystem", getConfigFile)
    implicit val ec: ExecutionContextExecutor = sys.classicSystem.dispatcher

    val tupleSpace = new TupleSpaceID(TUSOW_SYSTEM_NAME, TupleSpaceType.LOGIC)
    val listOfRef = ListBuffer[ActorRef[Message]]()

    Deployer.deploy(createBasicStateSensors(sys))
    Thread.sleep(5000)
    for (ref <- Deployer.getDevicesActorRefMap.toList)
      listOfRef.addOne(ref._2)

    val client = Client.createClient(sys.classicSystem, ec)

    client.createTupleSpace(tupleSpace).onComplete(response =>
      sys.systemActorOf(Behaviors.setup({
        context =>
          for (actor <- listOfRef.toList)
            actor ! Subscribe(context.self)

          Behaviors.receiveMessage({
            case Status(ref, value) =>
              val tuple = new Tuple(TUSOW_SYSTEM_NAME, "data(" + "'" + ref.toString + "'," + value.toString + ").")
              val writeResponse = Await.result[IOResponse](client.write(new WriteRequest(Some(tupleSpace), Some(tuple))), Duration(5000, TimeUnit.MILLISECONDS))
              println(writeResponse)
              Behaviors.same
            case _ => Behaviors.same
          })
      }), "TusowActor")
    )

  private def createBasicStateSensors(actorSys: ActorSystem[Message]): Graph[Device[Double]] =
    val basic1 = new BasicSensor[Double]("0", List.empty) with Public[Double] with Timer(Duration(1, TimeUnit.SECONDS))
    val basic2 = new BasicSensor[Double]("1", List.empty) with Public[Double] with Timer(Duration(2, TimeUnit.SECONDS))
    val basic3 = new BasicSensor[Double]("2", List.empty) with Public[Double] with Timer(Duration(3, TimeUnit.SECONDS))
    val basic4 = new BasicSensor[Double]("3", List.empty) with Public[Double] with Timer(Duration(4, TimeUnit.SECONDS))
    val graph = Graph[Device[Double]](
      basic1 -> basic2,
      basic1 -> basic3,
      basic2 -> basic4,
      basic3 -> basic1
    )
    graph

  private def getConfigFile: Config =
    val configFile = getClass.getClassLoader.getResource("application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    config

  private def setup(): Unit =
    Deployer.initSeedNodes()
    Deployer.addNodes(5)
    Server.start()








