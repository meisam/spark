package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

class GpuPartitionIterator[T <: Product : TypeTag]
(itr: Iterator[T], chunkCapacity: Int = 1 << 20)
  extends Serializable with Iterator[T] {

  override def hasNext: Boolean = {
    currentPosition < currentPartition.size || itr.hasNext
  }

  protected var currentPosition: Int = -1

  @transient protected val context: OpenCLContext = new OpenCLContext

  context.initOpenCL("/org/apache/spark/gpu/kernel.cl")

  protected val currentPartition: GpuPartition[T] = new GpuPartition[T](context, chunkCapacity)

  override def next(): T = {
    guaranteeFill
    val t: T = currentPartition(currentPosition)
    currentPosition += 1
    t
  }

  def guaranteeFill {
    if (currentPosition >= currentPartition.size || currentPosition < 0) {
      currentPartition.fill(itr)
      currentPosition = 0
    }
  }
}