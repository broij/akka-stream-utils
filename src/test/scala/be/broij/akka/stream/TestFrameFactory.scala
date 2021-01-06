package be.broij.akka.stream

import be.broij.akka.stream.operators.Window.FrameFactory

object TestFrameFactory extends FrameFactory[Int, TestFrame] {
  override def apply(): TestFrame = new TestFrame(List.empty)
  override def apply(item: Int): TestFrame = new TestFrame(List(item))
}
