package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag

class GpuFilteredPartitionIterator[T <: Product : ClassTag]
(itr: Iterator[T], columnTypes: Array[String],
 var openCLContext: OpenCLContext, colIndex: Int, operation: Int, value: Int, chunkCapacity: Int)
  extends GpuPartitionIterator[T](itr, columnTypes, chunkCapacity) {

  override def hasNext: Boolean = itr.hasNext
}


