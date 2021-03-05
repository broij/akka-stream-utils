package be.broij.akka.stream.operators.window

import akka.NotUsed
import akka.stream.scaladsl.Flow
import be.broij.akka.stream.operators.Window
import java.time.{Duration, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer

object TimedWindow {
  class Frame[T](payload: ListBuffer[T])(maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime)
      extends Window.Frame[T] {
    override def canAdd(item: T): Boolean =
      payload.headOption.forall {
        start =>
          maxPeriod >= FiniteDuration(Duration.between(timeOf(start), timeOf(item)).toNanos, TimeUnit.NANOSECONDS)
      }

    override def add(item: T): Window.Frame[T] = new Frame(payload += item)(maxPeriod, timeOf)
    override def payloadSeq: Seq[T] = payload.toList
    override def nonEmpty: Boolean = payload.nonEmpty
  }

  class FrameFactory[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime)
      extends Window.FrameFactory[T, Frame[T]] {
    override def apply(): Frame[T] = new Frame(ListBuffer.empty)(maxPeriod, timeOf)
    override def apply(item: T): Frame[T] = new Frame(ListBuffer(item))(maxPeriod, timeOf)
  }

  /**
   * Creates a flow working on streams where each element has an associated timestamp obtained with the function timeOf.
   * Let us call t(n) the timestamp associated to the nth element of such a stream. The flow assumes the elements are 
   * emitted in the order dictated by their timestamps: for each n < m we have that t(n) < t(m). It turns such a stream
   * of elements into a stream of windows. Each window is a sequence of timestamp-ordered elements giving the set of 
   * elements whose timestamps are included in a given time interval. Let l(n) be the index of the last element of the 
   * nth window. The index of the first element of the nth window is f(n) = l(n-1) + 1. The first window starts with the
   * first element of the stream: f(0) = 0. The nth window contains the elements that occurred in the
   * [t(f(n)), t(f(n)) + maxPeriod] time interval. The maxPeriod parameter defines the duration of the time intervals of
   * each window.
   */
  def apply[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, Seq[T], NotUsed] =
    Window(new FrameFactory[T](maxPeriod, timeOf))
}
