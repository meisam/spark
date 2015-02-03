package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.runtime.universe.TypeTag

class GpuFilteredPartitionIterator[T <: Product : TypeTag, V: TypeTag]
(parent: Iterator[GpuPartition[T]], var openCLContext: OpenCLContext,
 colIndex: Int, operation: ComparisonOperation.Value, value: V, chunkCapacity: Int)
  extends Serializable with Iterator[GpuPartition[T]] {

  override def hasNext: Boolean = itr.hasNext
}


