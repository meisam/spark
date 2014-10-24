package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.apache.spark.{Partition, TaskContext}
import org.jocl.CL._
import org.jocl._

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag

/**
 * Created by fathi on 10/1/14.
 */
class GpuFilteredRDD[T <: Product : ClassTag]
(prev: RDD[T], columnTypes: Array[String], colIndex: Int, operation: Int, value: Int,
 chunkCapacity: Int)
  extends GpuRDD[T](prev, columnTypes, chunkCapacity) {

  override def compute(split: Partition, context: TaskContext): GpuFilteredPartitionIterator[T] = {
    new GpuFilteredPartitionIterator(firstParent[T].iterator(split, context), columnTypes,
      openCLContext, colIndex, operation, value, chunkCapacity)
  }
}
