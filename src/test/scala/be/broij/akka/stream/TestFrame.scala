package be.broij.akka.stream

import be.broij.akka.stream.operators.Window.Frame
import scala.collection.immutable.Seq

class TestFrame(items: List[Int]) extends Frame[Int] {
  def canAdd(item: Int): Boolean = items.size < 2
  def add(item: Int) = new TestFrame(items :+ item)
  def payloadSeq: Seq[Int] = items
  def nonEmpty: Boolean = items.nonEmpty
}
