package be.broij.akka.stream

import be.broij.akka.stream.operators.SlidingWindow.FrameFactory

object TestSlidingFrameFactory extends FrameFactory[Int, TestSlidingFrame] {
  override def apply(): TestSlidingFrame = new TestSlidingFrame(List.empty)
}
