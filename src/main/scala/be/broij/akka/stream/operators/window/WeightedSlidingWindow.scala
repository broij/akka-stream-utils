package be.broij.akka.stream.operators.window

import akka.NotUsed
import akka.stream.scaladsl.Flow
import be.broij.akka.stream.operators.SlidingWindow
import scala.collection.mutable.ListBuffer

object WeightedSlidingWindow {
  class Frame[T, W: Numeric](payload: ListBuffer[T], totalWeight: W)(maxWeight: W, weightOf: T => W)
      extends WeightedWindow.Frame[T, W](payload, totalWeight)(maxWeight, weightOf) with SlidingWindow.Frame[T] {
    override def add(item: T): Frame[T, W] =
      new Frame(
        payload.append(item),
        Numeric[W].plus(totalWeight, weightOf(item))
      )(maxWeight, weightOf)

    override def shrink(item: T): Frame[T, W] = {
      val newPayload = payload.append(item)
      var newTotalWeight = Numeric[W].plus(totalWeight, weightOf(item))
      while (newPayload.nonEmpty && Numeric[W].gt(newTotalWeight, maxWeight)) {
        newTotalWeight = Numeric[W].minus(newTotalWeight, weightOf(newPayload.remove(0)))
      }
      new Frame(newPayload, newTotalWeight)(maxWeight, weightOf)
    }
  }

  class FrameFactory[T, W: Numeric](maxWeight: W, weightOf: T => W) extends SlidingWindow.FrameFactory[T, Frame[T, W]] {
    override def apply(): Frame[T, W] = new Frame(ListBuffer.empty, Numeric[W].zero)(maxWeight, weightOf)
  }

  /**
   * Creates a flow working on streams where each element has an associated weight obtained with the function weightOf.
   * Let us call w(n) the weight associated to the nth element of such a stream. The flow turns such a stream of 
   * elements into a stream of windows. Each window is the longest sequence of consecutive elements, kept in emission 
   * order, that starts with a given element and whose cumulative weight doesn't exceed maxWeight. The first window 
   * starts with the first element of the stream. Let l(n) be the index of the last element of the nth window. The 
   * n + 1 th window is fit to include the l(n) + 1 th element while repeating as much elements from the nth window as 
   * possible.
   */
  def apply[T, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, Seq[T], NotUsed] =
    SlidingWindow(new FrameFactory(maxWeight, weightOf))
}
