package it.pps.ddos.storage.tusow

import akka.NotUsed
import akka.stream.scaladsl.Source
import it.unibo.coordination.linda.core.TupleSpace
import it.unibo.coordination.linda.logic.{LogicMatch, LogicSpace, LogicTemplate, LogicTuple}
import it.unibo.coordination.tusow.grpc
import it.unibo.coordination.tusow.grpc.{IOResponse, IOResponseList, ReadOrTakeAllRequest, ReadOrTakeRequest, Template, Tuple, TupleSpaceID, TuplesList, TusowService, WriteAllRequest, WriteRequest}
import it.unibo.tuprolog.core.Term

import java.util.concurrent.CompletableFuture
import scala.compat.java8.FutureConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

object TusowAkkaLogicHandler{
    def apply(): TusowAkkaLogicHandler = new TusowAkkaLogicHandler()
}

class TusowAkkaLogicHandler extends TusowService {

    type LogicSpace = TupleSpace[LogicTuple, LogicTemplate, String, Term, LogicMatch]
    val logicSpaces = new scala.collection.mutable.HashMap[String, LogicSpace]

    override def validateTupleSpace(in: TupleSpaceID): Future[IOResponse] = Future.successful(IOResponse(logicSpaces.contains(in.id)))

    override def createTupleSpace(in: TupleSpaceID): Future[IOResponse] = {
        logicSpaces(in.id) = LogicSpace.local(in.id)
        Future.successful(IOResponse(response = true))
    }

    private def handleFutureRequest[A](space: LogicSpace)(failureHandler: () => Future[A])(successHandler: () => Future[A]): Future[A] = {
        if (space == null) {
            failureHandler()
        } else {
            successHandler()
        }
    }

    override def write(in: WriteRequest): Future[IOResponse] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        handleFutureRequest(space)(() => Future.successful(IOResponse(response = false, message = "Tuple space not found")))(() => space.write(in.tuple.get.value).toScala.map(f => IOResponse(response = true, message = f.toString)))
    }

    private def handleReadOrTakeRequest[A](in: ReadOrTakeRequest)(readOrTake: (LogicSpace, String, Long) => Future[A]): Future[A] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        handleFutureRequest(space)(() => Future.failed(new IllegalArgumentException("Tuple space not found")))(() => readOrTake(space, in.template.textualTemplate.get.regex, timeout))
    }

    override def read(in: ReadOrTakeRequest): Future[Tuple] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        handleReadOrTakeRequest(in)((space, template, timeout) => space.read(in.template.logicTemplate.getOrElse(Template.Logic()).query)
          .toScala.map(logicMatch => Tuple(key = logicMatch.getTemplate.toString, value = logicMatch.getTuple.orElse(LogicTuple.of("")).getValue.toString)))
    }

    override def take(in: ReadOrTakeRequest): Future[Tuple] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        if (space == null) {
            Future.successful(null)
        } else {
            space.take(in.template.logicTemplate.getOrElse(Template.Logic()).query).toScala.map(logicMatch => Tuple(key = logicMatch.getTemplate.toString, value = logicMatch.getTuple.orElse(LogicTuple.of("")).getValue.toString))
        }
    }

    override def writeAll(in: WriteAllRequest): Future[IOResponseList] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        if (space == null) {
            Future.successful(null)
        } else {
            val futures = in.tuplesList.get.tuples.map(t => space.write(t.value))
            var ioResponseList = scala.Seq[IOResponse]()
            TusowGRPCCommons.processWriteAllFutures(futures)(f => f.getValue.toString)
        }
    }

    override def readAll(in: ReadOrTakeAllRequest): Future[TuplesList] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        if (space == null) {
            Future.successful(null)
        } else {
            val futures = in.templates.logicTemplateList.get.queries.map(t => space.read(t.query))
            var tuplesList = scala.Seq[Tuple]()
           futures.foldLeft(CompletableFuture.allOf(futures.head))(CompletableFuture.allOf(_, _)).toScala.map(_ => {
                futures.foreach(f => {
                    tuplesList = tuplesList :+ Tuple(f.get().getTemplate.toString, f.get().getTuple.get().getValue.toString)
                })
                TuplesList(tuplesList)
           })
        }
    }


    override def takeAll(in: ReadOrTakeAllRequest): Future[TuplesList] = {
        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
        if (space == null) {
            Future.successful(null)
        } else {
            val futures = in.templates.logicTemplateList.get.queries.map(t => space.take(t.query))
            var tuplesList = scala.Seq[Tuple]()
            futures.foldLeft(CompletableFuture.allOf(futures.head))(CompletableFuture.allOf(_,_)).toScala.map(_ => {
                futures.foreach(f => {
                    tuplesList = tuplesList :+ Tuple(f.get().getTemplate.toString, f.get().getTuple.get().getValue.toString)
                })
                TuplesList(tuplesList)
            })
        }
    }

    override def writeAllAsStream(in: WriteAllRequest): Source[IOResponse, NotUsed] = {
        logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id) match {
            case null => Source.empty
            case space @ _ => {
                val futures = in.tuplesList.get.tuples.map(t => space.write(t.value))
                futures.foldLeft(CompletableFuture.allOf(futures.head))(CompletableFuture.allOf(_, _)).get()
                val result: Seq[LogicTuple] = Await.result(Future.sequence(futures.map(_.toScala)), Int.MaxValue.seconds)
                val iterable = result.map(f => IOResponse(response = true, message = f.getValue.toString))
                Source.apply[IOResponse](scala.collection.immutable.Iterable[IOResponse](iterable:_*))
            }
        }
    }

    override def readAllAsStream(in: ReadOrTakeAllRequest): Source[Tuple, NotUsed] = {
        logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id) match {
            case null => Source.empty
            case space@_ => {
                val futures = in.templates.logicTemplateList.get.queries.map(t => space.read(t.query))
                TusowGRPCCommons.joinFutures(futures)
                Source(futures.map(f => Tuple(f.get().getTemplate.toString, f.get().getTuple.get().getValue.toString)).toList)
            }
        }
    }

    override def takeAllAsStream(in: ReadOrTakeAllRequest): Source[Tuple, NotUsed] = {
        logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id) match {
            case null => Source.empty
            case space@_ => {
                val futures = in.templates.logicTemplateList.get.queries.map(t => space.take(t.query))
                TusowGRPCCommons.joinFutures(futures)
                Source(futures.map(f => Tuple(f.get().getTemplate.toString, f.get().getTuple.get().getValue.toString)).toList)
            }
        }
    }
}

//    override def writeAllAsStream(in: WriteAllRequest): Source[IOResponse, Future[NotUsed]] = {
//        val space = logicSpaces(in.tupleSpaceID.getOrElse(TupleSpaceID("")).id)
//        if (space == null) {
//            Source.futureSource(Future.successful(Source.empty))
//        } else {
//            val futures = in.tuplesList.get.tuples.map(t => space.write(t.value))
//            Source.futureSource(
//                futures.foldLeft(CompletableFuture.allOf(futures.head))((f1, f2) => CompletableFuture.allOf(f1, f2)).thenApplyAsync(_ => {
//                    Source(futures.map(f => IOResponse(response = true, message = f.get.toString)))
//                }).toScala
//            )
//        }
//    }