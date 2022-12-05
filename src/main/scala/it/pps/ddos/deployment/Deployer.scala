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