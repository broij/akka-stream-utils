package be.broij.akka.stream.operators.window

import akka.NotUsed
import akka.stream.scaladsl.Flow
import be.broij.akka.stream.operators.Window
import scala.collection.mutable.ListBuffer

object WeightedWindow {
  class Frame[T, W: Numeric](payload: ListBuffer[T], totalWeight: W)(maxWeight: W, weightOf: T => W)
      extends Window.Frame[T] {
    override def canAdd(item: T): Boolean = Numeric[W].lteq(Numeric[W].plus(totalWeight, weightOf(item)), maxWeight)
    override def add(item: T): Frame[T, W] =
      new Frame(
        payload.append(item),
        Numeric[W].plus(totalWeight, weightOf(item))
      )(maxWeight, weightOf)

    override def payloadSeq: Seq[T] = payload.toSeq
    override def nonEmpty: Boolean = payload.nonEmpty
  }

  class FrameFactory[T, W: Numeric](maxWeight: W, weightOf: T => W) extends Window.FrameFactory[T, Frame[T, W]] {
    override def apply(): Frame[T, W] = new Frame(ListBuffer.empty, Numeric[W].zero)(maxWeight, weightOf)
    override def apply(item: T): Frame[T, W] = new Frame(ListBuffer(item), weightOf(item))(maxWeight, weightOf)
  }

  /**
   * Creates a flow working on streams where each element has an associated weight obtained with the function weightOf.
   * Let us call w(n) the weight associated to the nth element of such a stream. The flow turns such a stream of 
   * elements into a stream of windows. Each window is the longest sequence of consecutive elements, kept in emission 
   * order, that starts with a given element and whose cumulative weight doesn't exceed maxWeight. The first window 
   * starts with the first element of the stream. Let l(n) be the index of the last element of the nth window. The index
   * of the first element of the nth window is f(n) = l(n-1) + 1.
   */
  def apply[T, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, Seq[T], NotUsed] =
    Window(new FrameFactory[T, W](maxWeight, weightOf))
}
