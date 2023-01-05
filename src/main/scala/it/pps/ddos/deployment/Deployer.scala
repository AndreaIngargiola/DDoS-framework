package it.pps.ddos.deployment

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, Join}
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.Device
import it.pps.ddos.device.DeviceProtocol.{Message, Subscribe}

import scala.collection.{immutable, mutable}

object Deployer:
  private case class ActorSysWithActor(actorSystem: ActorSystem[InternSpawn], numberOfActorSpawned: Int)

  private case class InternSpawn(id: String, behavior: Behavior[Message])

  private val orderedActorSystemRefList = mutable.ListBuffer.empty[ActorSysWithActor]

  private var devicesActorRefMap = Map.empty[String, ActorRef[Message]]

  /**
   * Initialize the seed nodes and the cluster
   */
  def initSeedNodes(): Unit =
    ActorSystem(Behaviors.empty, "ClusterSystem", setupClusterConfig("2551"))
    ActorSystem(Behaviors.empty, "ClusterSystem", setupClusterConfig("2552"))


  /**
   * Add N nodes to the cluster
   * @param numberOfNode the number of nodes to add
   */
  def addNodes(numberOfNode: Int): Unit =
    for (i <- 1 to numberOfNode)
        val as = createActorSystem("ClusterSystem")
        Thread.sleep(300)
        orderedActorSystemRefList += ActorSysWithActor(as, 0)

  private def createActorSystem(id: String): ActorSystem[InternSpawn] =
    ActorSystem(Behaviors.setup(
      context =>
        Behaviors.receiveMessage { msg =>
          msg match
            case InternSpawn(id, behavior) =>
              devicesActorRefMap = Map((id, context.spawn(behavior, id))) ++ devicesActorRefMap
              Behaviors.same
        }
    ), id, setupClusterConfig("0"))

  private def deploy[T](devices: Device[T]*): Unit =
    devices.foreach(dev =>
      val actorRefWithInt = orderedActorSystemRefList.filter(_.actorSystem.ref == getMinSpawnActorNode).head
      actorRefWithInt.actorSystem.ref ! InternSpawn(dev.id, dev.behavior())
      actorRefWithInt.numberOfActorSpawned + 1
    )

  /**
   * Deploys a graph of devices into the cluster.
   * Every edge of the graph represents a device that subscribes to another device.
   * Each device is deployed on the node with the minimum number of deployed devices.
   * @param devicesGraph The graph of devices to deploy
   * @tparam T The type of messages exchanged between devices
   */
  def deploy[T](devicesGraph: Graph[Device[T]]): Unit =
    var alreadyDeployed = Set[Device[T]]()
    devicesGraph @-> ((k,edges) =>
      if(!alreadyDeployed.contains(k))
        deploy(k)
        alreadyDeployed = alreadyDeployed + k
      edges.filter(!alreadyDeployed.contains(_)).foreach { e =>
        deploy(e)
        alreadyDeployed = alreadyDeployed + k
      }
    )
    devicesGraph @-> ((k, v) => v.map(it => devicesActorRefMap.get(it.id)).filter(_.isDefined).foreach(device => devicesActorRefMap(k.id).ref ! Subscribe(device.get.ref)))

  private def setupClusterConfig(port: String): Config =
    val hostname = "127.0.0.1"
    ConfigFactory.parseString(String.format("akka.remote.artery.canonical.hostname = \"%s\"%n", hostname)
      + String.format("akka.remote.artery.canonical.port=" + port + "%n")
      + String.format("akka.management.http.hostname=\"%s\"%n",hostname)
      + String.format("akka.management.http.port=" + port.replace("255", "855") + "%n")
      + String.format("akka.management.http.route-providers-read-only=%s%n", "false")
      + String.format("akka.remote.artery.advanced.tcp.outbound-client-hostname=%s%n", hostname)
      + String.format("akka.cluster.jmx.multi-mbeans-in-same-jvm = on"))
      .withFallback(ConfigFactory.load("application.conf"))

  private def getMinSpawnActorNode: ActorRef[InternSpawn] =
    orderedActorSystemRefList.minBy(x => x.numberOfActorSpawned).actorSystem