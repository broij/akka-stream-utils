package be.broij.akka.stream.operators

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import be.broij.akka.stream.operators.SlidingWindow.{Frame, FrameFactory}
import scala.collection.immutable.Seq

class SlidingWindow[T, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F])
    extends GraphStage[FlowShape[T, Seq[T]]] {
  protected val in: Inlet[T] = Inlet[T]("slidingWindow.in")
  protected val out: Outlet[Seq[T]] = Outlet[Seq[T]]("slidingWindow.out")

  override def shape: FlowShape[T, Seq[T]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var frame: Frame[T] = frameFactory()

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val item = grab(in)
          if (frame.canAdd(item)) {
            frame = frame.add(item)
            pull(in)
          } else {
            push(out, frame.payloadSeq)
            frame = frame.shrink(item)
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (frame.nonEmpty) emit(out, frame.payloadSeq)
          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
}

object SlidingWindow {
  /**
    * A frame represents a window being assembled.
    */
  trait Frame[T] {
    /**
      * False if the item can’t be added to the frame, true otherwise.
      */
    def canAdd(item: T): Boolean

    /**
      * Creates a new frame containing all the items in the frame plus the given item.
      */
    def add(item: T): Frame[T]

    /**
      * Gives the content of the frame as a sequence of elements.
      */
    def payloadSeq: Seq[T]

    /**
      * Creates a new frame containing as many items as possible from this frame plus the given item.
      */
    def shrink(item: T): Frame[T]

    /**
      * True if the frame is not empty, false otherwise.
      */
    def nonEmpty: Boolean
  }

  trait FrameFactory[T, F <: Frame[T]] {
    /**
      * Creates an empty frame.
      */
    def apply(): F
  }

  /**
    * Creates a flow turning streams of elements into streams of windows. Each window is a sequence of elements. To
    * build the windows, the flow consumes the elements one after the others. It starts with an empty frame F obtained
    * with the FrameFactory and tries to add each element it consumes to that frame. If an element can’t be added to the
    * frame, the window it represents is emitted as a sequence of elements and the frame is then fit to contain that
    * element to pursue the process of windowing the stream.
    */
  def apply[T, F <: Frame[T]](implicit frameFactory: FrameFactory[T, F]): Flow[T, Seq[T], NotUsed] =
    Flow.fromGraph(new SlidingWindow[T, F])
}
