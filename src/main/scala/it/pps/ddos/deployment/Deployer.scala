package it.pps.ddos.deployment

import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.device.DeviceProtocol.{Message, Subscribe}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, Join}
import it.pps.ddos.deployment.graph.Graph
import it.pps.ddos.device.Device
import it.pps.ddos.grouping.Tag

import scala.collection.{immutable, mutable}

object Deployer:
  private case class ActorSysWithActor(actorSystem: ActorSystem[InternSpawn], numberOfActorSpawned: Int)

  private case class InternSpawn(id: String, behavior: Behavior[Message])

  private val orderedActorSystemRefList = mutable.ListBuffer.empty[ActorSysWithActor]

  private var cluster: Option[Cluster] = None

  private var devicesActorRefMap = Map.empty[String, ActorRef[Message]]

  def initSeedNodes(): Unit =
    ActorSystem(Behaviors.empty, "ClusterSystem", setupClusterConfig("2551"))
    ActorSystem(Behaviors.empty, "ClusterSystem", setupClusterConfig("2552"))
    

  def addNodes(numberOfNode: Int): Unit =
    for (i <- 1 to numberOfNode)
        val as = createActorSystem("ClusterSystem")
        Thread.sleep(300)
        orderedActorSystemRefList += ActorSysWithActor(as, 0)

  private def createActorSystem(id: String): ActorSystem[InternSpawn] =
    println("Creating actor system " + id)
    ActorSystem(Behaviors.setup(
      context =>
        Behaviors.receiveMessage { msg =>
          msg match
            case InternSpawn(id, behavior) =>
              devicesActorRefMap = Map((id, context.spawn(behavior, "Actuator"))) ++ devicesActorRefMap
              Behaviors.same
        }
    ), id, setupClusterConfig("0"))

  private def deploy[T](devices: Device[T]*): Unit =
    devices.foreach(dev =>
      val actorRefWithInt = orderedActorSystemRefList.filter(_.actorSystem.ref == getMinSpawnActorNode).head
      actorRefWithInt.actorSystem.ref ! InternSpawn(dev.id, dev.behavior())
      actorRefWithInt.numberOfActorSpawned + 1
    )

  def deploy[T](devicesGraph: Graph[Device[T]]): Unit =
    devicesGraph @-> ((k,_) => deploy(k))
    devicesGraph @-> ((k, v) => v.map(it => devicesActorRefMap.get(it.id))
                                 .filter(_.isDefined)
                                 .foreach(device => devicesActorRefMap(k.id).ref ! Subscribe(device.get.ref)))
    //----------------------------------------------------------------
    var groups: Map[Tag, List[ActorRef[Message]]] = Map.empty
    devicesGraph @-> ((k, _) => groups = combineIterables(groups, (k.getTags() zip List.fill(k.getTags().length)(List(devicesActorRefMap(k.id).ref))).toMap))
    for((tag, sources) <- groups) yield deploy

  private def combineIterables[K, V](a: Map[K, List[V]], b: Map[K, List[V]]): Map[K, List[V]] =
    a ++ b.map { case (k, v) => k -> (v ++ a.getOrElse(k, List.empty)) }

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