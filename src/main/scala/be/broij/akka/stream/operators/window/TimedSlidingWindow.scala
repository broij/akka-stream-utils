package be.broij.akka.stream.operators.window

import akka.NotUsed
import akka.stream.scaladsl.Flow
import be.broij.akka.stream.operators.SlidingWindow
import java.time.{Duration, ZonedDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer

object TimedSlidingWindow {
  class Frame[T](payload: ListBuffer[T])(maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime)
      extends TimedWindow.Frame[T](payload)(maxPeriod, timeOf) with SlidingWindow.Frame[T] {
    override def add(item: T): Frame[T] = new Frame(payload += item)(maxPeriod, timeOf)

    override def shrink(item: T): Frame[T] = {
      val newPayload = payload += item
      var newPeriod = timeDiff(item)

      while (newPayload.nonEmpty && newPeriod > maxPeriod) {
        newPayload.remove(0)
        newPeriod = timeDiff(item)
      }

      new Frame(newPayload)(maxPeriod, timeOf)
    }

    protected def timeDiff(item: T): FiniteDuration = payload.headOption.map {
      start => FiniteDuration(Duration.between(timeOf(start), timeOf(item)).toNanos, TimeUnit.NANOSECONDS)
    }.getOrElse(FiniteDuration.apply(0, TimeUnit.NANOSECONDS))
  }


  class FrameFactory[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime)
      extends SlidingWindow.FrameFactory[T, Frame[T]] {
    override def apply(): Frame[T] = new Frame(ListBuffer.empty)(maxPeriod, timeOf)
  }

  /**
   * Creates a flow working on streams where each element has an associated timestamp obtained with the function timeOf. 
   * Let us call t(n) the timestamp associated to the nth element of such a stream. The flow assumes the elements are 
   * emitted in the order dictated by their timestamps: for each n < m we have that t(n) < t(m). It turns such a stream
   * of elements into a stream of windows. Each window is a sequence of timestamp-ordered elements giving the set of 
   * elements whose timestamps are included in a given time interval. The first window starts with the first element of 
   * the stream. Let f(n) be the index of the first element of the nth window. Such window contains the elements that 
   * occurred in the [t(f(n)), t(f(n)) + maxPeriod] time interval. The maxPeriod parameter defines the duration of the
   * time intervals of each window. Let l(n) be the index of the last element of the nth window. The n + 1 th window is 
   * fit to include the l(n) + 1 th element while repeating as much elements from the nth window as possible.
   */
  def apply[T](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, Seq[T], NotUsed] =
    SlidingWindow(new FrameFactory(maxPeriod, timeOf))
}
