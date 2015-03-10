
package org.apache.spark.rdd

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.runtime.universe.TypeTag

class GpuAggregationRDD[T <: Product : TypeTag, TP <: Product: TypeTag]
(parentRdd: GpuRDD[TP], aggregations: Array[AggregationExp],
 capacity: Int, numPartitions: Int)
  extends GpuRDD[T](parentRdd, capacity, numPartitions) {

  override def getPartitions: Array[Partition] = {
    firstParent[GpuPartition[TP]].partitions
  }

  override val partitioner = parentRdd.partitioner // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {
    val parentPartitionItr = parent[GpuPartition[TP]](0).iterator(split, context)

    parentPartitionItr.map {
      parentPartition =>
        val partition = new GpuAggregationPartition[T, TP](context.getOpenCLContext, parentPartition,
          aggregations, capacity)
        partition.aggregate()
        partition
    }
  }

}
