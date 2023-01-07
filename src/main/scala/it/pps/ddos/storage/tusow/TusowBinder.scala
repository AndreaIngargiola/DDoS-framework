package it.pps.ddos.storage.tusow

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.{Device, Public, Timer}
import it.pps.ddos.device.DeviceProtocol.{Message, PropagateStatus, Status, Subscribe}
import it.pps.ddos.device.sensor.BasicSensor
import it.pps.ddos.storage.tusow.TusowBinder.{ACTOR_SYSTEM_NAME, TUSOW_SYSTEM_NAME, getConfigFile}
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
  private final val ACTOR_SYSTEM_NAME = "ClusterSystem"

  private def setupDeployer(numNodes: Int): Unit =
    Deployer.initSeedNodes()
    Deployer.addNodes(numNodes)
    Server.start()

  private def getConfigFile: Config =
    val configFile = getClass.getClassLoader.getResource("application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    config

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

  def apply(sys: ActorSystem[Message] = ActorSystem(Behaviors.empty, ACTOR_SYSTEM_NAME, getConfigFile)) =
    implicit val ec: ExecutionContextExecutor = sys.classicSystem.dispatcher

    val tupleSpace = new TupleSpaceID(TUSOW_SYSTEM_NAME, TupleSpaceType.LOGIC)

    setupDeployer(5)
    Deployer.deploy(createBasicStateSensors(sys))
    Thread.sleep(5000)

    val actorList = Deployer.getDevicesActorRefMap.values.toList
    val client = Client.createClient(sys.classicSystem, ec)

    client.createTupleSpace(tupleSpace).onComplete(response =>
      sys.systemActorOf(Behaviors.setup({
        context =>
          actorList.foreach(actor => actor ! Subscribe(context.self))

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

/* Runnable TusowBinder actor for debug */
//object TusowBinderMain extends App:
  //TusowBinder()








