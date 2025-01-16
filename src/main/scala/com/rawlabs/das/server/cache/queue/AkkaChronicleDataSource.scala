/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.server.cache.queue

import akka.actor.typed._
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl._
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.util.Timeout
import net.openhft.chronicle.queue.{ChronicleQueue, ExcerptAppender, ExcerptTailer}

import java.io.{Closeable, File}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * REQUIREMENTS & DESIGN NOTES
 *
 * This code implements a system with a single data producer and multiple concurrent readers, backed by a Chronicle
 * Queue for durable, log-based storage. It is wrapped by an Akka-based “SharedDataSource” concept (here,
 * `AkkaChronicleDataSource`), which manages production and consumption of data, subject to the following constraints
 * and guarantees:
 *
 * 1) Single Producer
 *   - Exactly one `DataProducingTask[T]` is run, encapsulated by an actor (`ChronicleDataSourceActor`).
 *   - Once started, the producer (actor) continuously appends data into the Chronicle Queue until one of these
 *     conditions occurs: a) The iterator is exhausted (EOF). b) An error is thrown during data production. c) We
 *     voluntarily stop if no consumers are present for a configured “grace period.”
 *   - No second producer is permitted. All data originates from a single, shared iterator.
 *
 * 2) Multiple Independent Readers
 *   - Any number of readers (0, 1, or many) can appear and disappear at arbitrary times.
 *   - Each reader uses its own “tailer” on the same Chronicle Queue. In this Akka Streams design, each reader is
 *     represented by a `Source[T, _]` which pulls data at its own pace.
 *   - Readers do not affect each other’s progress and can be at different positions in the data.
 *   - A reader stops by simply cancelling/closing its stream when it no longer needs data.
 *
 * 3) Readers Always Start From the Beginning
 *   - By design, each new reader’s tailer is positioned at the start of the Chronicle Queue, ensuring it never misses
 *     data that was already appended.
 *   - This allows late-joining readers to consume all previously produced data from the beginning.
 *
 * 4) Readers May Stop At Will
 *   - Each reader (Akka stream) can terminate on its own by completing or failing the stage.
 *   - This supports partial consumption scenarios where some readers only need a subset of the overall data stream.
 *
 * 5) Avoid Producing Too Much Data (Demand-Driven)
 *   - A key requirement is to avoid producing an unbounded backlog if no reader is consuming. In the Akka-based
 *     version, we address this by:
 *     - Having the producer actor schedule periodic checks (“ticks”).
 *     - Producing data in small batches (configured by `batchSize`).
 *     - Stopping production entirely if there are no active consumers for a set grace period.
 *   - While not strictly a push-on-demand model (like “backpressure” in some streaming libraries), the combination of
 *     scheduled batch production and the grace period logic ensures we do not keep generating data endlessly with zero
 *     readers.
 *
 * 6) Grace Period for the Producer
 *   - Once the last reader closes, the producer stays “alive” for a grace period (e.g. 5 minutes).
 *   - If another reader attaches during that time, production resumes from where it left off.
 *   - If no reader appears before the grace timer expires, the producer: a) Writes a “VOLUNTARY_STOP” control message
 *     to the queue. b) Closes itself and ceases further production.
 *   - This prevents indefinite production and resource usage when no one is consuming the data.
 *
 * 7) Producer Stop vs. New Reader
 *   - Once the producer has fully stopped (due to EOF, error, or voluntary stop), no new readers can subscribe.
 *     Attempting to do so returns `None` from `getReader()`.
 *   - This design choice ensures that once the system is shut down, it does not accept new consumers, simplifying
 *     lifecycle management.
 *
 * 8) Strict Concurrency Requirements & No Edge-Case Races
 *   - In this Akka-based approach, we rely on the actor’s single-threaded mailbox to serialize access to shared data
 *     (the iterator and signals). The “producer” manipulates Chronicle storage from within one actor, eliminating many
 *     concurrency pitfalls.
 *   - Multiple readers each have their own tailer, which Chronicle Queue supports in a thread-safe manner for
 *     concurrent reads. This is safe as Chronicle Queue is designed for single-writer, multiple-reader usage.
 *
 * 9) Finality Once Producer Closes
 *   - After the producer writes EOF, ERROR, or VOLUNTARY_STOP, existing readers eventually see that control message and
 *     complete/terminate their streams.
 *   - No further data is appended, and no new consumers are allowed to join.
 *   - This ensures a well-defined shutdown process and prevents partial data writes or ambiguous end states.
 *
 * ** Summary & Guarantees **
 *   - A single producer actor, multiple readers each with their own Chronicle tailer.
 *   - Readers see the entire data stream from the beginning if they wish.
 *   - Readers can close freely; the producer automatically stops if nobody is reading for a configured grace period.
 *   - Once stopped, the system rejects new readers.
 *   - Concurrency is carefully managed via the Akka actor model and Chronicle Queue’s single-writer / multiple-reader
 *     design.
 *
 * ** Akka & ChronicleQueue-Specific Implementation Notes **
 *
 *   - **Akka Actor** (`ChronicleDataSourceActor`):
 *     - Periodically ticks (`producerInterval`) to produce data in batches from `DataProducingTask[T]`.
 *     - Maintains an `activeConsumers` count to detect when no one is reading. Starts a grace timer upon reaching zero.
 *       Cancels that timer if a new consumer arrives in time.
 *     - Appends data items to the Chronicle Queue (via `ChronicleStorage[T]`) and signals control messages for EOF,
 *       ERROR, or VOLUNTARY_STOP.
 *     - Shuts down and closes the queue in `postStop()`.
 *
 *   - **ChronicleStorage[T]**:
 *     - Wraps ChronicleQueue operations in a small utility class for appending data items or special control messages.
 *       Creates new tailers (one per consumer) starting at the beginning of the queue.
 *     - No concurrency issues since writes happen from a single actor. Reads happen from each consumer’s tailer.
 *       ChronicleQueue is safe for multiple tailers to read concurrently.
 *
 *   - **Akka Streams GraphStage** (`ChronicleSourceGraphStage[T]`):
 *     - Implements a polling loop for each reader (consumer). Each has its own ExcerptTailer.
 *     - On “pull,” it tries to read from the queue. If no data is present, it schedules a timer to check again soon
 *       (e.g., in 20ms).
 *     - If it encounters a control message (EOF, ERROR, VOLUNTARY_STOP), it completes or fails the stream accordingly.
 *     - Notifies the producer actor when the reader terminates, decrementing the `activeConsumers`.
 *
 *   - **Usage**:
 *     - Construct `new AkkaChronicleDataSource[T](task, queueDir, codec, ...)`.
 *     - Call `.getReader()` to obtain an optional `Source[T, _]`. If the producer is active (or has just finished EOF
 *       but not forced closed), you get `Some(Source)`. Otherwise `None`.
 *     - Materialize the source in an Akka Stream to consume data. You can have any number of sources simultaneously
 *       reading.
 *     - Once no consumers remain for the grace period, the producer stops itself.
 *     - You can also forcibly shut down everything by calling `.close()`, which sends a `PoisonPill` to the actor and
 *       closes the queue.
 */

/**
 * Utilities & Data Classes (same as before but combined for clarity):
 */
trait CloseableIterator[T] extends Iterator[T] with AutoCloseable {
  def close(): Unit
}

object CloseableIterator {
  def apply[T](it: Iterator[T] with Closeable): CloseableIterator[T] = {
    new CloseableIterator[T] {
      override def hasNext: Boolean = it.hasNext
      override def next(): T = it.next()
      override def close(): Unit = it.close()
    }
  }

  def wrap[T](it: Iterator[T], onClose: => Unit): CloseableIterator[T] = {
    new CloseableIterator[T] {
      override def hasNext: Boolean = it.hasNext
      override def next(): T = it.next()
      override def close(): Unit = onClose
    }
  }
}

trait DataProducingTask[T] {
  def run(): CloseableIterator[T]
}

trait Codec[T] {
  def write(out: net.openhft.chronicle.wire.WireOut, value: T): Unit
  def read(in: net.openhft.chronicle.wire.WireIn): T
}

class BasicCloseableIterator[T](iter: Iterator[T], closeFunc: () => Unit) extends CloseableIterator[T] {
  override def hasNext: Boolean = iter.hasNext
  override def next(): T = iter.next()
  override def close(): Unit = closeFunc()
}

class ChronicleStorage[T](queueDir: File, codec: Codec[T]) extends AutoCloseable {

  System.setProperty("disable.single.threaded.check", "true")

  private val queue: ChronicleQueue = ChronicleQueue.singleBuilder(queueDir).build()
  private val appender: ExcerptAppender = queue.createAppender()

  def appendData(elem: T): Unit = {
    appender.writeDocument { w =>
      w.write("data").marshallable { inner =>
        codec.write(inner, elem)
      }
    }
  }

  def signalEOF(): Unit = {
    appender.writeDocument { w =>
      w.write("control").text("EOF")
    }
  }

  def signalVoluntaryStop(): Unit = {
    appender.writeDocument { w =>
      w.write("control").text("VOLUNTARY_STOP")
    }
  }

  def signalError(ex: Throwable): Unit = {
    appender.writeDocument { w =>
      w.write("control").text("ERROR")
      w.write("message").text(ex.getMessage)
    }
  }

  def newTailerToStart(): ExcerptTailer = queue.createTailer().toStart

  override def close(): Unit = queue.close()
}

/**
 * The typed actor that manages the data production from `task` into `storage`, with a grace period for no consumers and
 * ability to subscribe.
 */
object ChronicleDataSource {

  // Outbound lifecycle events (if you want to notify another typed actor)
  sealed trait DataSourceLifecycleEvent
  final case class DataProductionComplete(sizeInBytes: Long) extends DataSourceLifecycleEvent
  final case class DataProductionError(msg: String) extends DataSourceLifecycleEvent
  final case object DataProductionVoluntaryStop extends DataSourceLifecycleEvent

  // Commands (inbound)
  sealed trait ChronicleDataSourceCommand

  // A request to subscribe. We'll reply via `replyTo` with Subscribed(...) or AlreadyStopped.
  final case class RequestSubscribe(replyTo: ActorRef[SubscribeResponse]) extends ChronicleDataSourceCommand

  // A consumer's stream has ended.
  final case class ConsumerTerminated(consumerId: Long) extends ChronicleDataSourceCommand

  // Internal messages for scheduling
  case object ProducerTick extends ChronicleDataSourceCommand
  case object GraceTimerExpired extends ChronicleDataSourceCommand

  // Command to forcibly stop the actor
  case object StopDataSource extends ChronicleDataSourceCommand

  // Subscribe response messages
  sealed trait SubscribeResponse
  final case class Subscribed(consumerId: Long) extends SubscribeResponse
  final case object AlreadyStopped extends SubscribeResponse

  // Producer states
  sealed trait ProducerState
  case object Running extends ProducerState
  case object Eof extends ProducerState
  case object ErrorState extends ProducerState
  case object VoluntaryStop extends ProducerState

  /**
   * Create a typed Behavior for the data source.
   */
  def apply[T](
      task: DataProducingTask[T],
      storage: ChronicleStorage[T],
      batchSize: Int,
      gracePeriod: FiniteDuration,
      producerInterval: FiniteDuration,
      callbackRef: Option[ActorRef[DataSourceLifecycleEvent]] = None): Behavior[ChronicleDataSourceCommand] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        new ChronicleDataSourceBehavior[T](
          context,
          timers,
          task,
          storage,
          batchSize,
          gracePeriod,
          producerInterval,
          callbackRef).running()
      }
    }
}

/**
 * Internal logic class. We store mutable fields for simplicity, but we remain in typed style.
 */
private class ChronicleDataSourceBehavior[T](
    ctx: ActorContext[ChronicleDataSource.ChronicleDataSourceCommand],
    timers: TimerScheduler[ChronicleDataSource.ChronicleDataSourceCommand],
    task: DataProducingTask[T],
    storage: ChronicleStorage[T],
    batchSize: Int,
    gracePeriod: FiniteDuration,
    producerInterval: FiniteDuration,
    callbackRef: Option[ActorRef[ChronicleDataSource.DataSourceLifecycleEvent]]) {

  import ChronicleDataSource._
  import ctx.log

  // mutable state
  private var state: ProducerState = Running
  private var iterOpt: Option[CloseableIterator[T]] = None
  private var activeConsumers: Int = 0
  private val nextConsumerId = new AtomicLong(0L)
  private var graceTimerActive = false

  // Start production ticks
  timers.startTimerAtFixedRate(ProducerTick, ProducerTick, producerInterval)

  // Possibly schedule grace if no consumers
  checkGracePeriod()

  def running(): Behavior[ChronicleDataSourceCommand] = Behaviors
    .receive[ChronicleDataSourceCommand] { (context, message) =>
      message match {

        case RequestSubscribe(replyTo) =>
          handleSubscribe(replyTo)
          Behaviors.same

        case ConsumerTerminated(cid) =>
          activeConsumers -= 1
          log.info(s"Consumer $cid terminated; activeConsumers=$activeConsumers")
          checkGracePeriod()
          Behaviors.same

        case ProducerTick =>
          if (state == Running) {
            produceSomeData()
          }
          Behaviors.same

        case GraceTimerExpired =>
          ctx.log.info(s"Grace period expired, activeConsumers=$activeConsumers, state=$state")
          if (activeConsumers == 0 && state == Running) {
            doVoluntaryStop()
          }
          Behaviors.same

        case StopDataSource =>
          // We'll do a normal stop => triggers postStop for cleanup
          Behaviors.stopped { () =>
            postStopCleanup()
          }
      }
    }
    .receiveSignal { case (context, PostStop) =>
      postStopCleanup()
      Behaviors.stopped
    }

  // Subscribe logic
  private def handleSubscribe(replyTo: ActorRef[SubscribeResponse]): Unit = {
    state match {
      case Running =>
        activeConsumers += 1
        cancelGraceTimer()
        val cid = nextConsumerId.getAndIncrement()
        replyTo ! Subscribed(cid)

      case Eof =>
        // Let them read backlog
        activeConsumers += 1
        cancelGraceTimer()
        val cid = nextConsumerId.getAndIncrement()
        replyTo ! Subscribed(cid)

      case ErrorState | VoluntaryStop =>
        replyTo ! AlreadyStopped
    }
  }

  private def produceSomeData(): Unit = {
    var count = 0
    try {
      val it = iterOpt.getOrElse {
        val newIter = task.run()
        log.info(s"Opened new iterator. hasNext=${newIter.hasNext}")
        iterOpt = Some(newIter)
        newIter
      }

      while (count < batchSize && it.hasNext) {
        val item = it.next()
        storage.appendData(item)
        count += 1
      }

      if (!it.hasNext) {
        log.info("Reached EOF, writing EOF marker.")
        storage.signalEOF()
        it.close()
        iterOpt = None
        state = Eof
        timers.cancel(ProducerTick)

        callbackRef.foreach { ref =>
          ref ! DataProductionComplete(sizeInBytes = 0L)
        }
      }

    } catch {
      case NonFatal(e) =>
        log.error("Producer error, stopping.", e)
        storage.signalError(e)
        iterOpt.foreach(_.close())
        iterOpt = None
        state = ErrorState
        timers.cancel(ProducerTick)

        callbackRef.foreach { ref =>
          ref ! DataProductionError(e.getMessage)
        }
    }
  }

  private def doVoluntaryStop(): Unit = {
    log.info(s"No consumers for $gracePeriod, voluntarily stopping production.")
    storage.signalVoluntaryStop()
    iterOpt.foreach(_.close())
    iterOpt = None
    state = VoluntaryStop
    timers.cancel(ProducerTick)

    callbackRef.foreach { ref =>
      ref ! DataProductionVoluntaryStop
    }
  }

  // Grace timer handling
  private def checkGracePeriod(): Unit = {
    log.debug(s"Checking grace period: $activeConsumers consumers, state=$state, graceTimerActive=$graceTimerActive")
    if (activeConsumers == 0 && state == Running && !graceTimerActive) {
      log.info(s"No consumers, starting grace period of $gracePeriod.")
      timers.startSingleTimer(GraceTimerExpired, GraceTimerExpired, gracePeriod)
      graceTimerActive = true
    }
  }
  private def cancelGraceTimer(): Unit = {
    log.info("Cancelling grace timer.")
    timers.cancel(GraceTimerExpired)
    graceTimerActive = false
  }

  private def postStopCleanup(): Unit = {
    log.info("ChronicleDataSource typed actor stopping, cleaning up.")
    timers.cancelAll()
    iterOpt.foreach(_.close())
    storage.close()
  }
}

/**
 * The GraphStage remains typed-friendly by referencing a typed ActorRef. Everything else is the same logic as your
 * original untyped stage.
 */
class ChronicleSourceGraphStage[T](
    actor: ActorRef[ChronicleDataSource.ChronicleDataSourceCommand],
    codec: Codec[T],
    storage: ChronicleStorage[T],
    consumerId: Long,
    pollInterval: FiniteDuration = 20.millis)(implicit ec: ExecutionContext)
    extends GraphStage[SourceShape[T]] {

  private val out: Outlet[T] = Outlet("ChronicleSourceGraphStage.out")
  override val shape: SourceShape[T] = SourceShape(out)

  private val PollTimerKey = "POLL_TIMER"

  override def createLogic(attrs: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with OutHandler {

      private var tailer: ExcerptTailer = _
      private var completed: Boolean = false

      override def preStart(): Unit = {
        tailer = storage.newTailerToStart()
      }

      private def tryReadWhileAvailable(): Unit = {
        while (!completed && isAvailable(out)) {
          val dc = tailer.readingDocument()
          if (!dc.isPresent) {
            dc.close()
            scheduleOnce(PollTimerKey, pollInterval)
            return
          } else {
            try {
              val wire = dc.wire()
              val sb = new java.lang.StringBuilder()
              wire.readEventName(sb)
              val eventName = sb.toString

              eventName match {
                case "data" =>
                  wire.read("data").marshallable { inner =>
                    val elem = codec.read(inner)
                    push(out, elem)
                  }

                case "control" =>
                  val ctrl = wire.read("control").text()
                  ctrl match {
                    case "EOF" | "VOLUNTARY_STOP" =>
                      completeStage()
                      completed = true
                    case "ERROR" =>
                      val msg = wire.read("message").text()
                      failStage(new RuntimeException(msg))
                      completed = true
                    case _ =>
                      completeStage()
                      completed = true
                  }

                case _ =>
                  completeStage()
                  completed = true
              }

            } finally {
              dc.close()
            }
          }
        }
      }

      override def onTimer(timerKey: Any): Unit = {
        if (!completed && timerKey == PollTimerKey) {
          tryReadWhileAvailable()
        }
      }

      override def onPull(): Unit = {
        if (!completed) {
          tryReadWhileAvailable()
        }
      }

      override def postStop(): Unit = {
        // Notify the typed producer
        actor ! ChronicleDataSource.ConsumerTerminated(consumerId)
        tailer.close()
        super.postStop()
      }

      setHandler(out, this)
    }
}

/**
 * The typed facade that spawns a ChronicleDataSource typed actor, returns a Source via typed ask, and a `close()`
 * method to stop it.
 */
class AkkaChronicleDataSource[T](
    task: DataProducingTask[T],
    queueDir: File,
    codec: Codec[T],
    batchSize: Int = 1000,
    gracePeriod: FiniteDuration = 5.minutes,
    producerInterval: FiniteDuration = 0.millis,
    callbackRef: Option[ActorRef[ChronicleDataSource.DataSourceLifecycleEvent]] = None)(implicit
    system: ActorSystem[_],
    mat: Materializer,
    ec: ExecutionContext) {

  import ChronicleDataSource._

  private val storage = new ChronicleStorage[T](queueDir, codec)

  private val producerRef: ActorRef[ChronicleDataSourceCommand] =
    system.systemActorOf(
      ChronicleDataSource[T](task, storage, batchSize, gracePeriod, producerInterval, callbackRef),
      name = s"chronicle-datasource-${java.util.UUID.randomUUID()}")

  def actorRef: ActorRef[ChronicleDataSource.ChronicleDataSourceCommand] = producerRef

  def getReader()(implicit timeout: Timeout): Future[Option[Source[T, _]]] = {
    // Typed ask:
    producerRef
      .ask[SubscribeResponse](replyTo => RequestSubscribe(replyTo))
      .map {
        case Subscribed(cid) =>
          val stage = new ChronicleSourceGraphStage[T](producerRef, codec, storage, cid)
          Some(Source.fromGraph(stage))

        case AlreadyStopped =>
          None
      }
      .recover { case _ => None }
  }

  /** Stop the typed actor. postStopCleanup will close storage. */
  def close(): Unit = {
    // Send a StopDataSource message. The child will do Behaviors.stopped & run postStopCleanup.
    producerRef ! StopDataSource
  }
}
