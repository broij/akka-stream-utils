package be.broij.akka.stream

import akka.actor.ActorSystem
import akka.NotUsed
import akka.stream.KillSwitches
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit}
import be.broij.akka.stream.SourceExtensions.{AnycastSourceConversion, AnycastWithPrioritiesSourceConversion, BalanceSourceConversion, BroadcastSourceConversion, CaptureMaterializedValuesSourceConversion, ConcatenateSourceConversion, DistinctKeySourceConversion, FilterConsecutivesSourceConversion, JoinFairlySourceConversion, JoinSourceConversion, JoinWithPrioritiesSourceConversion, PartitionSourceConversion, ReorderSourceConversion, SwitchSourceConversion, TimedSlidingWindowSourceConversion, TimedWindowSourceConversion, WeightedSlidingWindowSourceConversion, WeightedWindowSourceConversion}
import be.broij.akka.stream.operators.{SlidingWindow, Window}
import com.typesafe.config.ConfigFactory
import java.time.ZonedDateTime
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.annotation.tailrec
import scala.concurrent.duration.DurationDouble
import scala.collection.immutable.Seq
import scala.math.Numeric.IntIsIntegral
import scala.util.Random

class OperatorsSpec(_system: ActorSystem) extends TestKit(_system)
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  def this() =
    this(ActorSystem(
      "RoutersSpec",
      ConfigFactory.parseString(
        """
          akka {
            loggers = ["akka.testkit.TestEventListener"]
            loglevel = "WARNING"
          }
          """
      )
    ))

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "Concatenate" must {
    "concatenate all substreams in order" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .concatenate.runWith(TestSink.probe[Int])
        .request(10).expectNextN(1 to 10).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed(new Exception("Fake exception"))
        .concatenate.runWith(TestSink.probe)
        .request(1).expectError()
    }

    "fail when a substream fails" in {
      Source(List(Source(1 to 5), Source.failed(new Exception("Fake exception")), Source.single(6)))
        .concatenate.runWith(TestSink.probe[Int])
        .request(6).expectNextN(1 to 5).expectError()
    }
  }

  "Switch" must {
    "concatenate all substreams and switch to the next substream as soon as it is available" in {
      val probe =
        Source(List(Source.repeat(1), Source.empty, Source.repeat(2), Source.single(3)))
          .switch.runWith(TestSink.probe[Int])

      var last = 0
      do {
        probe.request(1)
        probe.expectNextOrComplete() match {
          case Left(_) if last != 3 => assert(false)
          case Right(e) =>
            if (e < last) assert(false)
            else last = e
        }
      } while (last != 3)
      probe.request(1)
      probe.expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed(new Exception("Fake exception"))
        .switch.runWith(TestSink.probe)
        .request(1).expectError()
    }

    "fail when a substream fails" in {
      @tailrec
      def validate(probe: TestSubscriber.Probe[Int], i: Int): Assertion =
        if (i > 5) {
          assert(false)
        } else {
          probe.expectNextOrError() match {
            case Left(_) => assert(true)
            case Right(_) => validate(probe, i + 1)
          }
        }

      validate(
        Source(List(Source(1 to 5), Source.failed(new Exception("Fake exception")), Source.single(6)))
          .switch.runWith(TestSink.probe[Int])
          .request(6),
        0
      )
    }
  }

  "Join" must {
    "merge all substreams" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .join(None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNextUnorderedN(1 to 10).expectComplete()
    }

    "merge all substreams in order when breadth is set to 1" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .join(Some(1)).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNextN(1 to 10).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed(new Exception("Fake exception"))
        .join(None).throttle(1, 0.1.seconds).runWith(TestSink.probe)
        .request(1).expectError()
    }

    "fail when a substream fails" in {
      @tailrec
      def validate(probe: TestSubscriber.Probe[Int]): Assertion =
        probe.expectNextOrError() match {
          case Left(_) => assert(true)
          case Right(_) => validate(probe)
        }

      validate(
        Source(List(Source(1 to 5), Source.failed(new Exception("Fake exception")), Source.single(6)))
          .join(None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
          .request(7)
      )
    }
  }

  "JoinFairly" must {
    "merge all substreams in a fair manner" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .joinFairly(2, None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNext(1, 2, 6, 7, 8, 3, 4, 9, 10, 5).expectComplete()
    }

    "merge all substreams in order when breadth is set to 1" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .joinFairly(2, Some(1)).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNextN(1 to 10).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed(new Exception("Fake exception"))
        .joinFairly(2, None).throttle(1, 0.1.seconds).runWith(TestSink.probe)
        .request(1).expectError()
    }

    "fail when a substream fails" in {
      @tailrec
      def validate(probe: TestSubscriber.Probe[Int]): Assertion =
        probe.expectNextOrError() match {
          case Left(_) => assert(true)
          case Right(_) => validate(probe)
        }

      validate(
        Source(List(Source(1 to 5), Source.failed(new Exception("Fake exception")), Source.single(6)))
          .joinFairly(2, None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
          .request(7)
      )
    }
  }

  "JoinWithPriorities" must {
    "merge all substreams taking into account the elements priorities" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .joinWithPriorities(identity[Int], None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNextUnorderedN(1 to 10).expectComplete()
    }

    "merge all substreams in order when breadth is set to 1" in {
      Source(List(Source(1 to 5), Source.empty, Source.single(6), Source(7 to 10)))
        .joinWithPriorities(identity[Int], Some(1)).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
        .request(11).expectNextN(1 to 10).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed(new Exception("Fake exception"))
        .joinWithPriorities(identity[Int], None).throttle(1, 0.1.seconds).runWith(TestSink.probe)
        .request(1).expectError()
    }

    "fail when a substream fails" in {
      @tailrec
      def validate(probe: TestSubscriber.Probe[Int]): Assertion =
        probe.expectNextOrError() match {
          case Left(_) => assert(true)
          case Right(_) => validate(probe)
        }

      validate(
        Source(List(Source(1 to 5), Source.failed(new Exception("Fake exception")), Source.single(6)))
          .joinWithPriorities(identity[Int], None).throttle(1, 0.1.seconds).runWith(TestSink.probe[Int])
          .request(7)
      )
    }
  }

  "Aggregate" must {
    "send downstream the elements sent by the producers with an ordering defined by the specified flattening " +
      "operator" in {

    }
  }

  "Window" must {
    "assemble windows of two elements when using a frame that does so" in {
      Source(1 to 11)
        .via(Window(TestFrameFactory)).runWith(TestSink.probe[Seq[Int]])
        .request(7).expectNext(Seq(1, 2), Seq(3, 4), Seq(5, 6), Seq(7, 8), Seq(9, 10), Seq(11)).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .via(Window(TestFrameFactory)).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "SlidingWindow" must {
    "assemble sliding windows of two elements when using a frame that does so" in {
      Source(1 to 11)
        .via(SlidingWindow(TestSlidingFrameFactory)).runWith(TestSink.probe[Seq[Int]])
        .request(11).expectNext(
          Seq(1, 2), Seq(2, 3), Seq(3, 4), Seq(4, 5), Seq(5, 6), Seq(6, 7), Seq(7, 8), Seq(8, 9), Seq(9, 10),
          Seq(10, 11)
        ).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .via(SlidingWindow(TestSlidingFrameFactory)).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "WeightedWindow" must {
    "assemble windows of two elements when weight of each elements is 1 and maxWeight is 2" in {
      Source(1 to 11)
        .weightedWindow(2, _ => 1).runWith(TestSink.probe[Seq[Int]])
        .request(7).expectNext(Seq(1, 2), Seq(3, 4), Seq(5, 6), Seq(7, 8), Seq(9, 10), Seq(11)).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .weightedWindow(2, _ => 1).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "WeightedSlidingWindow" must {
    "assemble sliding windows of two elements when weight of each elements is 1 and maxWeight is 2" in {
      Source(1 to 11)
        .weightedSlidingWindow(2, _ => 1).runWith(TestSink.probe[Seq[Int]])
        .request(11).expectNext(
          Seq(1, 2), Seq(2, 3), Seq(3, 4), Seq(4, 5), Seq(5, 6), Seq(6, 7), Seq(7, 8), Seq(8, 9), Seq(9, 10),
          Seq(10, 11)
        ).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .weightedSlidingWindow(2, _ => 1).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "TimedWindow" must {
    "assemble timed windows of two elements when each elements are spaced of 1 second and maxPeriod is 1 second" in {
      val baseTime = ZonedDateTime.now()
      Source(1 to 11)
        .timedWindow(1.seconds, n => baseTime.plusSeconds(n)).runWith(TestSink.probe[Seq[Int]])
        .request(7).expectNext(Seq(1, 2), Seq(3, 4), Seq(5, 6), Seq(7, 8), Seq(9, 10), Seq(11)).expectComplete()
    }

    "fail when upstream fails" in {
      val baseTime = ZonedDateTime.now()
      Source.failed[Int](new Exception("Fake exception"))
        .timedWindow(1.seconds, n => baseTime.plusSeconds(n)).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "TimedSlidingWindow" must {
    "assemble sliding timed windows of two elements when each elements are spaced of 1 second and maxPeriod is 1 " +
      "second" in {
      val baseTime = ZonedDateTime.now()
      Source(1 to 11)
        .timedSlidingWindow(1.seconds, n => baseTime.plusSeconds(n)).runWith(TestSink.probe[Seq[Int]])
        .request(11).expectNext(
          Seq(1, 2), Seq(2, 3), Seq(3, 4), Seq(4, 5), Seq(5, 6), Seq(6, 7), Seq(7, 8), Seq(8, 9), Seq(9, 10),
          Seq(10, 11)
        ).expectComplete()
    }

    "fail when upstream fails" in {
      val baseTime = ZonedDateTime.now()
      Source.failed[Int](new Exception("Fake exception"))
        .timedSlidingWindow(1.seconds, n => baseTime.plusSeconds(n)).runWith(TestSink.probe[Seq[Int]])
        .request(1).expectError()
    }
  }

  "CaptureMaterializedValues" must {
    "capture the materialized value of each substream and materialize them as a source of materialized values" in {
      val (mats, probe) = Source(List(
        Source(1 to 5).mapMaterializedValue(_ => 1),
        Source.empty.mapMaterializedValue(_ => 2),
        Source.single(6).mapMaterializedValue(_ => 3),
        Source(7 to 10).mapMaterializedValue(_ => 4)
      )).captureMaterializedValues
        .toMat(TestSink.probe[Source[Int, NotUsed]])(Keep.both).run()

      val probeMat = mats.runWith(TestSink.probe[Int])

      probe.request(5)
      probeMat.request(5).expectNextN(1 to 4).expectComplete()
    }

    "fail when upstream fails" in {
      val (mats, probe) = Source.failed[Source[Int, Int]](new Exception("Fake exception"))
        .captureMaterializedValues
        .toMat(TestSink.probe[Source[Int, NotUsed]])(Keep.both).run()

      val probeMat = mats.runWith(TestSink.probe[Int])

      probe.request(1)
      probeMat.request(1).expectError()
    }
  }

  "DistinctKey" must {
    "reduce each repetition of a key (K, K, .., K) to a single occurence of that key: (K)" in {
      Source(List(1, 2, 1, 3, 3, 4, 4, 4, 3, 4, 5, 5, 6, 6))
        .distinctKey(identity).runWith(TestSink.probe[Int])
        .request(10).expectNext(1, 2, 1, 3, 4, 3, 4, 5, 6).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .distinctKey(identity).runWith(TestSink.probe[Int])
        .request(1).expectError()
    }
  }

  "FilterConsecutives" must {
    "filter out all the consecutive elements that are asked to be filtered out" in {
      Source(List(1, 2, 3, 1, 2, 1, 2, 3, 4, 5, 6, 5, 1))
        .filterConsecutives((current, candidate) => current.forall(candidate > _)).runWith(TestSink.probe[Int])
        .request(7).expectNextN(1 to 6).expectComplete()
    }

    "fail when upstream fails" in {
      Source.failed[Int](new Exception("Fake exception"))
        .filterConsecutives((a, b) => false).runWith(TestSink.probe[Int])
        .request(1).expectError()
    }
  }

  "Reorder" must {
    "reorder windows of 2 elements when configured to operate on weighted windows of maxWeight of 2 and where each " +
      "element has a weight of 1" in {
      Source(List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
        .reorder[BigInt](BigInt(2),  _ => 1).runWith(TestSink.probe[Int])
        .request(11).expectNext(9, 10, 7, 8, 5, 6, 3, 4, 1, 2).expectComplete()
    }

    "reorder windows of 2 elements when configured to operate on timed windows of maxPeriod of 1s and where each " +
      "element is spaced of 1s" in {
      val baseTime = ZonedDateTime.now()
      Source(List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
        .reorder(1.seconds, n => baseTime.plusSeconds(10 - n)).runWith(TestSink.probe[Int])
        .request(11).expectNext(9, 10, 7, 8, 5, 6, 3, 4, 1, 2).expectComplete()
    }

    "fail when upstream fails and it is configured to operate on weighted windows" in {
      Source.failed[Int](new Exception("Fake exception"))
        .reorder[BigInt](BigInt(2), _ => 1).runWith(TestSink.probe[Int])
        .request(1).expectError()
    }

    "fail when upstream fails and it is configured to operate on timed windows" in {
      val baseTime = ZonedDateTime.now()
      Source.failed[Int](new Exception("Fake exception"))
        .reorder(1.seconds, n => baseTime.plusSeconds(10 - n)).runWith(TestSink.probe[Int])
        .request(1).expectError()
    }
  }

  "BroadcastWaitFastest" must {
    "send each element to all consumers without waiting for the slowest one when the buffer is full" in {
      val broadcastSource = Source(1 to 10).broadcast(
        restartSource = false, waitSlowest = false, 5, 1.seconds
      )

      val probe0 = broadcastSource.runWith(TestSink.probe[Int])
      val probe1 = broadcastSource.runWith(TestSink.probe[Int])
      probe1.request(6).expectNextN(1 to 6)
      probe0.request(1).expectNext(2)
      val probe2 = broadcastSource.runWith(TestSink.probe[Int])
      probe2.request(9).expectNextN(2 to 10)

      probe1.request(5).expectNextN(7 to 10).expectComplete()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe0.request(5).expectNextN(7 to 10).expectComplete()
    }

    "stop the source when all consumers unregister and then restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val broadcastSource = Source(1 to 10).broadcast(
        restartSource = true, waitSlowest = false, 5, 1.seconds
      )

      val probe0 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe1._2.request(6).expectNextN(1 to 6)
      probe0._2.request(1).expectNext(2)
      probe0._1.shutdown()
      probe0._2.expectComplete()
      probe1._2.request(4).expectNextN(7 to 10)
      probe1._1.shutdown()
      probe1._2.expectComplete()

      val probe2 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(11).expectNextN(1 to 10).expectComplete()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val broadcastSource = Source.failed[Int](new Exception("Fake exception")).broadcast(
        restartSource = true, waitSlowest = false, 5, 1.seconds
      )

      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectError()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "BroadcastWaitSlowest" must {
    "send each element to all consumers waiting for the slowest one when the buffer is full at it configured as " +
      "such" in {
      val broadcastSource = Source(1 to 10).broadcast(
        restartSource = false, waitSlowest = true, 5, 1.seconds
      )

      val probe0 = broadcastSource.runWith(TestSink.probe[Int])
      val probe1 = broadcastSource.runWith(TestSink.probe[Int])
      probe1.request(7).expectNextN(1 to 5).expectNoMessage()
      probe0.request(3).expectNextN(1 to 3)
      probe1.expectNext(6, 7)
      val probe2 = broadcastSource.runWith(TestSink.probe[Int])
      probe2.request(11).expectNextN(3 to 7).expectNoMessage()

      probe0.request(8).expectNextN(4 to 10).expectComplete()
      probe2.expectNextN(8 to 10).expectComplete()
      probe1.request(4).expectNextN(8 to 10).expectComplete()
      val probe3 = broadcastSource.runWith(TestSink.probe[Int])
      probe3.request(1).expectComplete()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "stop the source when all consumers unregister and then restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val broadcastSource = Source(1 to 10).broadcast(
        restartSource = true, waitSlowest = true, 5, 1.seconds
      )

      val probe0 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe1._2.request(6).expectNextN(1 to 5).expectNoMessage()
      probe1._1.shutdown()
      probe1._2.expectComplete()
      probe0._2.request(10).expectNextN(1 to 10)
      probe0._1.shutdown()
      probe0._2.expectComplete()

      val probe2 = broadcastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(11).expectNextN(1 to 10).expectComplete()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val broadcastSource = Source.failed[Int](new Exception("Fake exception")).broadcast(
        restartSource = true, waitSlowest = true, 5, 1.seconds
      )

      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectError()
      broadcastSource.runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "Anycast" must {
    "send each elements to any of its consumers" in {
      val anycastSource = Source.fromIterator[Int](() => Iterator.fill(10)(1)).anycast(restartSource = false, 1.seconds)

      val probe0 = anycastSource.runWith(TestSink.probe[Int])
      val probe1 = anycastSource.runWith(TestSink.probe[Int])
      val probe2 = anycastSource.runWith(TestSink.probe[Int])
      for (_ <- 0 until 10) {
        Random.nextInt(3) match {
          case 0 => probe0.request(1).expectNext(1).expectNoMessage()
          case 1 => probe1.request(1).expectNext(1).expectNoMessage()
          case 2 => probe2.request(1).expectNext(1).expectNoMessage()
        }
      }

      probe0.request(1).expectComplete()
      anycastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe1.request(1).expectComplete()
      probe2.request(1).expectComplete()
    }

    "stop the source when all consumers unregister and then restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val anycastSource = Source(1 to 10).anycast(restartSource = true, 1.seconds)

      val probe0 = anycastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = anycastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()

      probe0._2.request(4).expectNextN(1 to 4)
      probe0._1.shutdown()
      probe0._2.expectComplete()
      probe1._2.request(6).expectNextN(5 to 10)
      probe1._1.shutdown()
      probe1._2.expectComplete()

      val probe3 = anycastSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe3._2.request(11).expectNextN(1 to 10).expectComplete()
      anycastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe3._1.shutdown()
      anycastSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val anycastSource = Source.failed[Int](new Exception("Fake exception")).anycast(restartSource = false, 1.seconds)

      anycastSource.runWith(TestSink.probe[Int]).request(1).expectError()
      anycastSource.runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "AnycastWithPriorities" must {
    "send each elements to any of its consumers, taking the one with the highest priority if severall of them are " +
      "available" in {
      val (sourceProbe, source) = TestSource.probe[Int].preMaterialize()
      val anycastWithPrioritiesSource = source.anycastWithPriorities(restartSource = false, 1.seconds)

      val probe0 = anycastWithPrioritiesSource.withPriority(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = anycastWithPrioritiesSource.withPriority(2).runWith(TestSink.probe[Int])
      val probe2 = anycastWithPrioritiesSource.withPriority(3)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()

      probe0._2.request(1).expectNoMessage()
      probe1.request(1).expectNoMessage()
      probe2._2.request(1).expectNoMessage()
      sourceProbe.sendNext(1)
      probe1.expectNoMessage()
      probe2._2.expectNoMessage()
      probe0._2.expectNext(1)
      sourceProbe.sendNext(2)
      probe2._2.expectNoMessage()
      probe1.expectNext(2)
      probe0._2.request(1).expectNoMessage()
      sourceProbe.sendNext(3)
      probe2._2.expectNoMessage()
      probe0._2.expectNext(3)
      probe0._2.request(1)
      probe0._1.shutdown()
      probe0._2.expectComplete()
      sourceProbe.sendNext(4)
      probe2._2.expectNext(4)
      probe2._1.shutdown()
      probe2._2.request(1).expectComplete()
      probe1.request(1).expectNoMessage()
      sourceProbe.sendNext(5)
      probe1.expectNext(5)
      probe1.request(1).expectNoMessage()
      sourceProbe.sendComplete()
      anycastWithPrioritiesSource.withPriority(2).runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe1.expectComplete()
    }

    "stop the source when all consumers unregister and the restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val (sourceProbe, source) = TestSource.probe[Int].preMaterialize()
      val anycastWithPrioritiesSource = Source(1 to 10).anycastWithPriorities(restartSource = true, 1.seconds)

      val probe0 = anycastWithPrioritiesSource.withPriority(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = anycastWithPrioritiesSource.withPriority(2)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      sourceProbe.sendNext(1)
      probe0._2.request(9).expectNextN(1 to 9)
      probe0._1.shutdown()
      probe0._2.request(1).expectComplete()
      probe1._2.request(1).expectNext(10)
      probe1._1.shutdown()
      probe1._2.request(1).expectComplete()

      val probe2 = anycastWithPrioritiesSource.withPriority(3)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(11).expectNextN(1 to 10).expectComplete()
      anycastWithPrioritiesSource.withPriority(1).runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      anycastWithPrioritiesSource.withPriority(1).runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val anycastWithPrioritiesSource = Source.failed[Int](new Exception("Fake exception"))
        .anycastWithPriorities(restartSource = false, 1.seconds)

      anycastWithPrioritiesSource.withPriority(1).runWith(TestSink.probe[Int]).request(1).expectError()
      anycastWithPrioritiesSource.withPriority(1).runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "Balance" must {
    "send each elements to one of its consumers in a round robin fashion" in {
      val balanceSource = Source(1 to 10).balance(restartSource = false, 1, 1.seconds)

      val probe0 = balanceSource.runWith(TestSink.probe[Int])
      val probe1 = balanceSource.runWith(TestSink.probe[Int])
      val probe2 = balanceSource.runWith(TestSink.probe[Int])
      val probe3 = balanceSource.runWith(TestSink.probe[Int])
      Thread.sleep(100) //sleep a bit to make sure probe 4 is the last consumer registered
      val probe4 = balanceSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe0.request(11)
      probe1.request(11)
      probe2.request(11)
      probe3.request(11)
      val base0 = probe0.expectNext()
      val base1 = probe1.expectNext()
      val base2 = probe2.expectNext()
      val base3 = probe3.expectNext()
      probe0.expectNoMessage()
      probe1.expectNoMessage()
      probe2.expectNoMessage()
      probe3.expectNoMessage()
      probe4._1.shutdown()
      probe4._2.request(1).expectComplete()
      if (base0 == 1 || base0 == 2) probe0.expectNext(base0 + 4, base0 + 8).expectComplete()
      else probe0.expectNext(base0 + 4).expectComplete()
      if (base1 == 1 || base1 == 2) probe1.expectNext(base1 + 4, base1 + 8).expectComplete()
      else probe1.expectNext(base1 + 4).expectComplete()
      if (base2 == 1 || base2 == 2) probe2.expectNext(base2 + 4, base2 + 8).expectComplete()
      else probe2.expectNext(base2 + 4).expectComplete()
      if (base3 == 1 || base3 == 2) probe3.expectNext(base3 + 4, base3 + 8).expectComplete()
      else probe3.expectNext(base3 + 4).expectComplete()
      balanceSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "stop the source when all consumers unregister and the restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val balanceSource = Source(1 to 10).balance(restartSource = true, 1, 1.seconds)

      val probe0 = balanceSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe0._2.request(2).expectNext(1, 2)
      val probe1 = balanceSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      Thread.sleep(100) //sleep a bit to make sure probe1 is registered before probe0 shutdown
      probe0._1.shutdown()
      probe0._2.expectComplete()
      probe1._2.request(8).expectNextN(3 to 10)
      probe1._1.shutdown()
      probe1._2.expectComplete()

      val probe2 = balanceSource.viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(11).expectNextN(1 to 10).expectComplete()
      balanceSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      balanceSource.runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val balanceSource = Source.failed[Int](new Exception("Fake exception"))
        .balance(restartSource = false, 1, 1.seconds)

      balanceSource.runWith(TestSink.probe[Int]).request(1).expectError()
      balanceSource.runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "Partition wait fastest" must {
    "broadcast the elements of each partitions to its respective consumers without waiting for the slowest one " +
      "when the buffer is full" in {
      val partitionedSource = Source(List(1, 1, 2, 2, 1, 1, 2, 2, 1, 1))
        .partition(restartSource = false, identity[Int], waitSlowest = false, 5, 1.seconds)

      val probe0 = partitionedSource(1).runWith(TestSink.probe[Int])
      val probe1 = partitionedSource(1).runWith(TestSink.probe[Int])
      val probe2 = partitionedSource(2).runWith(TestSink.probe[Int])

      probe0.request(2).expectNext(1 , 1)
      probe1.request(2).expectNext(1 , 1)
      probe2.request(5).expectNext(2, 2, 2, 2).expectComplete()
      probe0.request(4).expectNext(1, 1, 1).expectComplete()
      probe1.request(4).expectNext(1, 1, 1).expectComplete()

      partitionedSource(2).runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "stop the source when all consumers unregister and the restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val partitionedSource = Source(List(1, 1, 2, 2, 1, 1, 2, 2, 1, 1))
        .partition(restartSource = true, identity[Int], waitSlowest = false, 5, 1.seconds)

      val probe0 = partitionedSource(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = partitionedSource(2)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe0._2.request(5).expectNext(1, 1, 1, 1, 1)
      probe1._2.request(1).expectNext(2)
      probe0._1.shutdown()
      probe0._2.expectComplete()
      probe1._1.shutdown()
      probe1._2.expectComplete()

      val probe2 = partitionedSource(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(6).expectNext(1, 1, 1, 1, 1, 1).expectComplete()
      partitionedSource(3).runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      partitionedSource(1).runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val partitionedSource = Source.failed[Int](new Exception("Fake exception"))
        .partition(restartSource = false, identity[Int], waitSlowest = false, 5, 1.seconds)

      partitionedSource(1).runWith(TestSink.probe[Int]).request(1).expectError()
      partitionedSource(2).runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }

  "Partition wait slowest" must {
    "broadcast the elements of each partitions to its respective consumers waiting for the slowest one when the " +
      "buffer is full" in {
      val partitionedSource = Source(List(1, 1, 2, 2, 1, 1, 2, 2, 1, 1))
        .partition(restartSource = false, identity[Int], waitSlowest = true, 5, 1.seconds)

      val probe0 = partitionedSource(1).runWith(TestSink.probe[Int])
      val probe1 = partitionedSource(1).runWith(TestSink.probe[Int])
      val probe2 = partitionedSource(2).runWith(TestSink.probe[Int])

      probe0.request(1).expectNext(1).expectNoMessage()
      probe1.request(1).expectNext(1).expectNoMessage()
      probe2.request(5).expectNext(2, 2).expectNoMessage()
      probe0.request(5).expectNext(1, 1, 1).expectNoMessage()
      probe1.request(5).expectNext(1, 1, 1, 1, 1).expectComplete()
      probe0.expectNext(1, 1).expectComplete()
      probe2.expectNext(2, 2).expectComplete()

      partitionedSource(2).runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "stop the source when all consumers unregister and the restart it as soon as there is a new registration when " +
      "restartSource is set to true" in {
      val partitionedSource = Source(List(1, 1, 2, 2, 1, 1, 2, 2, 1, 1))
        .partition(restartSource = true, identity[Int], waitSlowest = true, 5, 1.seconds)

      val probe0 = partitionedSource(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe1 = partitionedSource(2)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe0._2.request(5).expectNext(1, 1, 1)
      probe1._2.request(3).expectNext(2, 2, 2)
      probe0._2.expectNext(1, 1)
      probe0._1.shutdown()
      probe0._2.expectComplete()
      probe1._1.shutdown()
      probe1._2.expectComplete()

      val probe2 = partitionedSource(1)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      val probe3 = partitionedSource(2)
        .viaMat(KillSwitches.single)(Keep.right).toMat(TestSink.probe[Int])(Keep.both).run()
      probe2._2.request(6)
      probe3._2.request(5).expectNext(2, 2, 2, 2).expectComplete()
      probe2._2.expectNext(1, 1, 1, 1, 1, 1).expectComplete()
      partitionedSource(3).runWith(TestSink.probe[Int]).request(1).expectComplete()
      probe2._1.shutdown()
      probe3._1.shutdown()
      partitionedSource(1).runWith(TestSink.probe[Int]).request(1).expectComplete()
    }

    "fail when upstream fails" in {
      val partitionedSource = Source.failed[Int](new Exception("Fake exception"))
        .partition(restartSource = false, identity[Int], waitSlowest = true, 5, 1.seconds)

      partitionedSource(1).runWith(TestSink.probe[Int]).request(1).expectError()
      partitionedSource(2).runWith(TestSink.probe[Int]).request(1).expectError()
    }
  }
}
