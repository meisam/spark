package org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

class GpuPartitionIterator[T <: Product : TypeTag]
(itr: Iterator[T], capacity: Int = 1 << 20)
  extends Serializable with Iterator[GpuPartition[T]] {

  override def next(): GpuPartition[T] = {
    val nextDataChunk = itr.take(capacity)
    val optimalCapacity = Math.min(nextDataChunk.length, capacity)
    val currentPartition = new GpuPartition[T](null, optimalCapacity)
    currentPartition.fill(nextDataChunk)
    currentPartition
  }

  override def hasNext: Boolean = itr.hasNext
}