package be.broij.akka.stream

import be.broij.akka.stream.operators.Window.Frame

class TestFrame(items: List[Int]) extends Frame[Int] {
  def canAdd(item: Int): Boolean = items.size < 2
  def add(item: Int) = new TestFrame(items.appended(item) )
  def payloadSeq: Seq[Int] = items
  def nonEmpty: Boolean = items.nonEmpty
}
