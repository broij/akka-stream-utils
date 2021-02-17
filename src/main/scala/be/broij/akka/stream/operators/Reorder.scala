package be.broij.akka.stream.operators

import akka.NotUsed
import akka.stream.scaladsl.Flow
import be.broij.akka.stream.FlowExtensions.TimedWindowFlowConversion
import be.broij.akka.stream.operators.Window.{Frame, FrameFactory}
import be.broij.akka.stream.operators.window.{TimedWindow, WeightedWindow}
import java.time.ZonedDateTime
import scala.concurrent.duration.FiniteDuration

object Reorder {
  /**
    * Creates a flow working on streams of elements for which an [[Ordering]] implementation is provided. Referring to 
    * that ordering implementation, it tries to make sure the elements are emitted in ascending order. Since a stream is 
    * an infinite sequence, it cannot be fully sorted. Instead, the flow splits the stream into windows of elements, 
    * sorts each of them and concatenates their content in order. The order is thus guaranteed per window. To sort each 
    * window, the flow uses the default scala sorting algorithm, which uses [[java.util.Arrays.sort]]. To build the 
    * windows, the flow uses the flow produced by [[Window.apply]]. One can refer to its specification to get detailed 
    * information about the role of the parameters that weren't covered in this specification.
    */
  def apply[T: Ordering, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F]): Flow[T, T, NotUsed] =
    Flow[T].via(Window(frameFactory)).mapConcat(_.sorted)

  /**
    * Creates a flow working on streams of elements for which an [[Ordering]] implementation is provided. Referring to 
    * that ordering implementation, it tries to make sure the elements are emitted in ascending order. Since a stream is 
    * an infinite sequence, it cannot be fully sorted. Instead, the flow splits the stream into windows of elements, 
    * sorts each of them and concatenates their content in order. The order is thus guaranteed per window. To sort each 
    * window, the flow uses the default scala sorting algorithm, which uses [[java.util.Arrays.sort]]. To build the 
    * windows, the flow uses the flow produced by [[WeightedWindow.apply]]. One can refer to its specification to get 
    * detailed information about the role of the parameters that weren't covered in this specification.
    */
  def apply[T: Ordering, W: Numeric](maxWeight: W, weightOf: T => W): Flow[T, T, NotUsed] =
    Flow[T].via(WeightedWindow(maxWeight, weightOf)).mapConcat(_.sorted)

  /**
    * Creates a flow working on streams of elements for which an [[Ordering]] implementation is provided. Referring to 
    * that ordering implementation, it tries to make sure the elements are emitted in ascending order. Since a stream is 
    * an infinite sequence, it cannot be fully sorted. Instead, the flow splits the stream into windows of elements, 
    * sorts each of them and concatenates their content in order. The order is thus guaranteed per window. To sort each 
    * window, the flow uses the default scala sorting algorithm, which uses [[java.util.Arrays.sort]]. To build the 
    * windows, the flow uses the flow produced by [[TimedWindow.apply]]. One can refer to its specification to get 
    * detailed information about the role of the parameters that weren't covered in this specification.
    */
  def apply[T: Ordering](maxPeriod: FiniteDuration, timeOf: T => ZonedDateTime): Flow[T, T, NotUsed] =
    Flow[T].timedWindow(maxPeriod, timeOf).mapConcat(_.sorted)
}
