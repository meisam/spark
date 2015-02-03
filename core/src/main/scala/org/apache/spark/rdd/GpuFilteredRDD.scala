package org.apache.spark.rdd

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}


/**
 * Created by fathi on 10/1/14.
 */
class GpuFilteredRDD[T <: Product : ClassTag : TypeTag, V: TypeTag]
(prev: GpuRDD[T], colIndex: Int, operation: ComparisonOperation.Value, value: V,
 chunkCapacity: Int)
  extends RDD[GpuPartition[T]](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]]
  = {
    val iter: Iterator[GpuPartition[T]] = firstParent[GpuPartition[T]].iterator(split, context)

    iter.zipWithIndex.map { case (parent, index) =>
      val partition = new GpuFilteredPartition[T, V](
        null /*openCLContext*/ , index, colIndex, operation, value, chunkCapacity)
      partition.filter(parent)
      partition
    }
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner    // Since filter cannot change a partition's keys

}
