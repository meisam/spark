package org.apache.spark.rdd

import scala.reflect.runtime.universe.TypeTag

class GpuPartitionIterator[T <: Product : TypeTag]
(itr: Iterator[GpuPartition[T]], chunkCapacity: Int = 1 << 20)
  extends Serializable with Iterator[GpuPartition[T]] {

  override def hasNext: Boolean = {
    itr.hasNext
  }

  override def next(): GpuPartition[T] = {
    itr.next
  }
}