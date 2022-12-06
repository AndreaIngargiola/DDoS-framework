package it.pps.ddos.deployment

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor.typed.scaladsl.*
import akka.cluster.typed.*
import it.pps.ddos.device.DeviceProtocol.Message
import it.pps.ddos.device.actuator.Actuator

import scala.collection.mutable

object Deployer:
  private case class ActorSysWithActor(actorRef: ActorRef[InternSpawn], numberOfActorSpawned: Int)

  private case class InternSpawn(val behavior: Behavior[Message])

  private val orderedActorSystemRefList = mutable.ListBuffer.empty[ActorSysWithActor]

  private val cluster = Cluster(system = ActorSystem(Behaviors.same[Message], "ClusterSystem", setupClusterConfig("2551")))


  def init(numberOfNode: Int): Unit =
    for (i <- 1 to numberOfNode) {
      val actorSystem = ActorSystem(Behaviors.setup(
        context =>
          Behaviors.receiveMessage { msg =>
            msg match
              case InternSpawn(behavior) =>
                context.spawn(behavior, "Actuator")
                Behaviors.same
          }
      ), "ActorSystem-" + i)
      Cluster(system = actorSystem).manager ! Join(cluster.selfMember.address)
      orderedActorSystemRefList += ActorSysWithActor(actorSystem.ref, 0)
    }

  private def deploy(devices: Actuator[Message]*): Unit =
    devices.foreach(dev =>
      orderedActorSystemRefList.foreach(actorRefWithInt =>
        if actorRefWithInt.actorRef == getMinSpawnActorNode then
          actorRefWithInt.actorRef ! InternSpawn(dev.getBehavior())
          actorRefWithInt.numberOfActorSpawned + 1
      )
    )

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
    orderedActorSystemRefList.minBy(x => x.numberOfActorSpawned).actorRef