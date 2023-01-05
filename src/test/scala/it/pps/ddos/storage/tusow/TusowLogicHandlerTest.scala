package it.pps.ddos.storage.tusow

import akka.actor.ActorSystem
import it.pps.ddos.deployment.Deployer
import it.pps.ddos.storage.tusow.TusowLogicHandlerTest.tupleSpace
import it.pps.ddos.storage.tusow.client.Client
import it.unibo.coordination.tusow.grpc.{IOResponse, ReadOrTakeRequest, Template, Tuple, TupleSpaceID, TupleSpaceType, TusowServiceClient, WriteRequest}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object TusowLogicHandlerTest:

    val tupleSpace = new TupleSpaceID("ddos-storage", TupleSpaceType.LOGIC)
class TusowLogicHandlerTest extends AnyFlatSpec:

    "TusowLogicHandler" should "create a tuple space" in {
        Deployer.initSeedNodes()
        Server.start()
        implicit val sys: ActorSystem = ActorSystem("ClusterSystem")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val client = Client.createClient(sys, ec)
        val tupleSpace = new TupleSpaceID("ddos-storage", TupleSpaceType.LOGIC)
        client.createTupleSpace(tupleSpace).onComplete(response => {
            assert(response.isSuccess)
        })
    }

    it should "check if a tuple space exists" in {
        initTusowServer()
        implicit val sys: ActorSystem = ActorSystem("ClusterSystem")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val client = Client.createClient(sys, ec)
        createTupleSpace(client).onComplete(response => {
            client.validateTupleSpace(tupleSpace).onComplete(response => {
                assert(response.isSuccess)
            })
        })
    }

    it should "write a tuple" in {
        initTusowServer()
        implicit val sys: ActorSystem = ActorSystem("ClusterSystem")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val client = Client.createClient(sys, ec)
        createTupleSpace(client).onComplete(response => {
            val tuple = new Tuple("test", "test")
            client.write(new WriteRequest(Some(tupleSpace), Some(tuple))).onComplete(response => {
                assert(response.isSuccess)
            })
        })
    }

    it should "read a tuple" in {
        initTusowServer()
        implicit val sys: ActorSystem = ActorSystem("TestSystem")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val client = Client.createClient(sys, ec)
        Thread.sleep(4000)
        Await.result(createTupleSpace(client), Duration(5000, TimeUnit.MILLISECONDS))
        val tuple = new Tuple("", "loves(romeo, juliet).")
        val readTemplate = new Template.Logic("loves(romeo, X)")
        val readOrTakeRequestTemplate = ReadOrTakeRequest.Template.LogicTemplate(readTemplate)
        println("write")
        val writeResponse = Await.result[IOResponse](client.write(new WriteRequest(Some(tupleSpace), Some(tuple))), Duration(5000, TimeUnit.MILLISECONDS))
        println("read")
        val readResponse = Await.result[Tuple](client.read(new ReadOrTakeRequest(Some(tupleSpace), readOrTakeRequestTemplate)), Duration(5000, TimeUnit.MILLISECONDS))
        println(readResponse.value)
        assert(readResponse.value == tuple.value)
    }

private def initTusowServer(): Unit =
    Deployer.initSeedNodes()
    Server.start()

private def createTupleSpace(client: TusowServiceClient): Future[IOResponse] =
    client.createTupleSpace(tupleSpace)