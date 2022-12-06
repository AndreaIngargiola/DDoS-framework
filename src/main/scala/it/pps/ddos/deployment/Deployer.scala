package it.pps.ddos.deployment

import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import it.pps.ddos.device.DeviceProtocol.{Message, Subscribe}
import it.pps.ddos.device.actuator.Actuator
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.cluster.typed.{Cluster, Join}
import it.pps.ddos.deployment.graph.Graph

import scala.collection.{immutable, mutable}

object Deployer:
  private case class ActorSysWithActor(actorSystem: ActorSystem[InternSpawn], numberOfActorSpawned: Int)

  private case class InternSpawn(id: String, behavior: Behavior[Message])

  private val orderedActorSystemRefList = mutable.ListBuffer.empty[ActorSysWithActor]

  private val cluster = Cluster(system = ActorSystem(Behaviors.same[Message], "ClusterSystem", setupClusterConfig("2551")))
  private var devicesActorRefMap = Map.empty[String, ActorRef[Message]]


  def init(numberOfNode: Int): Unit =
    for (i <- 1 to numberOfNode) {
      val actorSystem = ActorSystem(Behaviors.setup(
        context =>
          Behaviors.receiveMessage { msg =>
            msg match
              case InternSpawn(id, behavior) =>
                devicesActorRefMap = Map((id, context.spawn(behavior, "Actuator"))) ++ devicesActorRefMap
                Behaviors.same
          }
      ), "ActorSystem-" + i)
      Cluster(system = actorSystem).manager ! Join(cluster.selfMember.address)
      orderedActorSystemRefList += ActorSysWithActor(actorSystem, 0)
    }

  private def deploy(devices: Actuator[Message]*): Unit =
    devices.foreach(dev =>
      val actorRefWithInt = orderedActorSystemRefList.filter(_.actorSystem.ref == getMinSpawnActorNode).head
      actorRefWithInt.actorSystem.ref ! InternSpawn(dev.id, dev.getBehavior)
      actorRefWithInt.numberOfActorSpawned + 1
    )

  def deploy(devicesGraph: Graph[Actuator[Message]]): Unit =
    devicesGraph @-> ((k,_) => deploy(k))
    devicesGraph @-> ((k, v) => v.map(it => devicesActorRefMap.get(it.id)).filter(_.isDefined).foreach(device => devicesActorRefMap(k.id).ref ! Subscribe(device.get.ref)))

  private def setupClusterConfig(port: String): Config =
    val hostname = "127.0.0.1"
    ConfigFactory.parseString(String.format("akka.remote.artery.canonical.hostname = \"%s\"%n", hostname)
      + String.format("akka.remote.artery.canonical.port=" + port)
      + String.format("akka.management.http.hostname =" + hostname)
      + String.format("akka.management.http.port=" + port.replace("255", "855"))
      + String.format("akka.management.http.route-providers-read-only = %s%n", "false")
      + String.format("akka.remote.artery.advanced.tcp.outbound-client-hostname = %s%n", hostname))
      .withFallback(ConfigFactory.load())

  private def getMinSpawnActorNode: ActorRef[InternSpawn] =
    orderedActorSystemRefList.minBy(x => x.numberOfActorSpawned).actorSystem