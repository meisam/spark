package org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

class GpuPartitionIterator[T <: Product : TypeTag]
(itr: Iterator[T], capacity: Int = 1 << 20)
  extends Serializable with Iterator[GpuPartition[T]] {

  override def next(): GpuPartition[T] = {
    val currentPartition = new GpuPartition[T](null, capacity)
    currentPartition.fill(itr)
    currentPartition
  }

  override def hasNext: Boolean = itr.hasNext
}