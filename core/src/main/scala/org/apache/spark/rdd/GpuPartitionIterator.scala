package org.apache.spark.rdd

import scala.reflect.ClassTag

class GpuPartitionIterator[T <: Product : ClassTag]
(itr: Iterator[T], val columnTypes: Array[String], chunkCapacity: Int = 1 << 20)
  extends Serializable with Iterator[T] {

  override def hasNext: Boolean = {
    currentPosition < currentChunk.size || itr.hasNext
  }

  protected var currentPosition: Int = -1

  protected val currentChunk: GpuPartition[T] = new GpuPartition[T](columnTypes, chunkCapacity)

  override def next(): T = {
    guaranteeFill
    val t: T = currentChunk(currentPosition)
    currentPosition += 1
    t
  }

  def guaranteeFill {
    if (currentPosition >= currentChunk.size || currentPosition < 0) {
      currentChunk.fill(itr)
      currentPosition = 0
    }
  }
}