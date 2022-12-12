package it.pps.ddos.storage.tusow.client

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import it.unibo.coordination.tusow.TupleSpaceTypes
import it.unibo.coordination.tusow.grpc.{ReadOrTakeRequest, Template, Tuple, TupleSpaceID, TupleSpaceType, TusowServiceClient, WriteRequest}

import scala.concurrent.ExecutionContextExecutor

object Client:

    def testClient(): Unit =
        implicit val sys: ActorSystem = ActorSystem("HelloWorldClient")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val clientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8080).withTls(false)
        val client = TusowServiceClient(clientSettings)
        val tupleSpace = new TupleSpaceID("ddos-storage", TupleSpaceType.LOGIC)
        client.createTupleSpace(tupleSpace).onComplete(response => {
            if(response.isSuccess){
                val writeRequest = new WriteRequest(Some(tupleSpace), Some(new Tuple("a", "ciao")))
                client.write(writeRequest).onComplete(response => {
                    if(response.isSuccess){
                        println("Write successful")
                        val template = new Template.Logic("ciao")
                        val readRequest = new ReadOrTakeRequest(Some(tupleSpace), ReadOrTakeRequest.Template.LogicTemplate(template))
                        client.read(readRequest).onComplete(response => {
                            if(response.isSuccess){
                                println("Read successful")
                                println(response.get)
                            } else {
                                println("Read failed")
                                println(response.failed.get)
                            }
                        })
                    } else {
                        println("Write failed")
                    }
                })
            }
        })

    def testClientTextual(): Unit =
        implicit val sys: ActorSystem = ActorSystem("HelloWorldClient")
        implicit val ec: ExecutionContextExecutor = sys.dispatcher
        val clientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", 8080).withTls(false)
        val client = TusowServiceClient(clientSettings)
        val tupleSpace = new TupleSpaceID("ddos-storage", TupleSpaceType.TEXTUAL)
        client.createTupleSpace(tupleSpace).onComplete(response => {
            if (response.isSuccess) {
                val writeRequest = new WriteRequest(Some(tupleSpace), Some(new Tuple("a", "150")))
                client.write(writeRequest).onComplete(response => {
                    if (response.isSuccess) {
                        println("Write successful")
                        val template = new Template.Textual("150")
                        val readRequest = new ReadOrTakeRequest(Some(tupleSpace), ReadOrTakeRequest.Template.TextualTemplate(template))
                        client.read(readRequest).onComplete(response => {
                            if (response.isSuccess) {
                                println("Read successful")
                                println(response.get)
                            } else {
                                println("Read failed")
                                println(response.failed.get)
                            }
                        })
                    } else {
                        println("Write failed")
                    }
                })
            }
        })
