package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.runtime.universe.TypeTag

class GpuFilteredPartitionIterator[T <: Product : TypeTag, V: TypeTag]
(itr: Iterator[T], var openCLContext: OpenCLContext,
 colIndex: Int, operation: Int, value: V, chunkCapacity: Int)
  extends GpuPartitionIterator[T](itr, chunkCapacity) {

  override def hasNext: Boolean = itr.hasNext
}


