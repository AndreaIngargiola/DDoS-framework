package it.pps.ddos.storage.tusow

//#import


import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import scala.io.Source
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.ConnectionContext
import akka.http.scaladsl.Http
import akka.http.scaladsl.HttpsConnectionContext
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.pki.pem.DERPrivateKeyLoader
import akka.pki.pem.PEMDecoder
import com.typesafe.config.ConfigFactory
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.storage.tusow.TusowAkkaService
import it.pps.ddos.storage.tusow.client.Client
import it.unibo.coordination.tusow.grpc.TusowServiceHandler

import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
//#import


//#server
object Server {

    trait ServerCommand
    case class Start(tusowAkkaService: TusowAkkaService) extends ServerCommand
    case class Stop() extends ServerCommand


    def main(args: Array[String]): Unit = {
        Deployer.initSeedNodes()
        val conf = ConfigFactory.parseString("akka.http.server.preview.enable-http2 = on")
          .withFallback(ConfigFactory.defaultApplication())
        val system = ActorSystem[Any](Behaviors.receive((ctx, message) => {
            message match {
                case Start(service) =>
                    new Server(ctx.system.classicSystem, service).run()
                    Behaviors.same
                case Stop => Behaviors.stopped
            }
        }), "ClusterSystem", conf)
        system.ref ! Start(new TusowAkkaService(system))
        Thread.sleep(5000)
        Client.testClient()
//        Client.testClientTextual()
    }
}

class Server(system: akka.actor.ActorSystem, tusowAkkaService: TusowAkkaService) {

    def run(): Future[Http.ServerBinding] = {
        implicit val sys = system
        implicit val ec: ExecutionContext = system.dispatcher

        val service: HttpRequest => Future[HttpResponse] =
            TusowServiceHandler(tusowAkkaService)

        val bound: Future[Http.ServerBinding] = Http()
          .newServerAt(interface = "127.0.0.1", port = 8080)
          //.enableHttps(serverHttpContext)
          .bind(service)
          .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

        bound.onComplete {
            case Success(binding) =>
                val address = binding.localAddress
                //println("gRPC server bound to {}:{}", address.getHostString, address.getPort)
            case Failure(ex) =>
                //println("Failed to bind gRPC endpoint, terminating system", ex)
                system.terminate()
        }

        bound
    }
}