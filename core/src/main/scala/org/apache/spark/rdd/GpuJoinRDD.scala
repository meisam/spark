
package org.apache.spark.rdd


import org.apache.spark.{Partition, TaskContext}

import scala.reflect.runtime.universe.TypeTag

class GpuJoinRDD[T <: Product : TypeTag, TL <: Product : TypeTag, TR <: Product : TypeTag, U :
TypeTag]
(leftPartition: GpuRDD[TL], rightPartition: GpuRDD[TR],
 joinColumnIndexThis: Int, joinColumnIndexOther: Int,
 capacity: Int, numPartitions: Int)
  extends GpuRDD[T](leftPartition, capacity, numPartitions) {

  val rightPartitionBroadcast = context.broadcast(rightPartition.collect())

  override def getPartitions: Array[Partition] = {
    firstParent[GpuPartition[TL]].partitions
  }

  override val partitioner = leftPartition.partitioner // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {
    val leftPartitionItr = parent[GpuPartition[TL]](0).iterator(split, context)

    val results: Iterator[GpuJoinPartition[T, TL, TR, U]] = leftPartitionItr.map { leftPartition =>

      val rightPartitions = rightPartitionBroadcast.value
      rightPartitions.toIterator.map { rightPartition =>
        val partition = new GpuJoinPartition[T, TL, TR, U](context.getOpenCLContext, leftPartition,
          rightPartition, joinColumnIndexThis, joinColumnIndexOther, capacity)
        partition.join()
        partition
      }
    }.flatten

    results
  }
}
