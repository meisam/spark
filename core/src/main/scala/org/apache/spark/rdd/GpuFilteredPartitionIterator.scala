package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.runtime.universe.TypeTag

class GpuFilteredPartitionIterator[T <: Product : TypeTag, V: TypeTag]
(parent: Iterator[GpuPartition[T]], var openCLContext: OpenCLContext,
 colIndex: Int, operation: ComparisonOperation.Value, value: V, chunkCapacity: Int)
  extends Serializable with Iterator[GpuPartition[T]] {

 override def hasNext: Boolean = {
  parent.hasNext
 }

 protected var currentPosition: Int = -1

 @transient protected val context: OpenCLContext = new OpenCLContext

 context.initOpenCL("/org/apache/spark/gpu/kernel.cl")

 protected val currentPartition = new GpuFilteredPartition[T, V](
  context, currentPosition, colIndex, operation, value, chunkCapacity)

 override def next(): GpuPartition[T] = {
  guaranteeFill
   currentPosition += 1
   new GpuFilteredPartition[T, V](
     context, currentPosition, colIndex, operation, value, chunkCapacity)
 }

 def guaranteeFill {
   val parentPartition: GpuPartition[T] = parent.next()
   currentPartition.filter(parentPartition)
   currentPosition = +1
 }

}


