package be.broij.akka.stream

import be.broij.akka.stream.operators.SlidingWindow.Frame
import scala.collection.immutable.Seq

class TestSlidingFrame(items: List[Int]) extends Frame[Int] {
  def canAdd(item: Int): Boolean = items.size < 2
  def add(item: Int) = new TestSlidingFrame(items :+ item)
  def shrink(item: Int): Frame[Int] =
    if (items.size < 2) new TestSlidingFrame(items :+ item)
    else new TestSlidingFrame(items.tail :+ item)
  def payloadSeq: Seq[Int] = items
  def nonEmpty: Boolean = items.nonEmpty
}