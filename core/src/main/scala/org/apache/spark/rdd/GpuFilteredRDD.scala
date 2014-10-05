package org.apache.spark.rdd

import org.apache.spark.{TaskContext, Partition}

import scala.reflect.ClassTag


/**
 * Created by fathi on 10/3/14.
 */
class GpuFilteredRDD[T <: Product : ClassTag]
(prev: RDD[T], override val columnTypes: Array[String]
 , colIndex: Int, op: String, value: Int)
  extends GpuRDD[T](prev, columnTypes) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext): Iterator[RDDChunk[T]] = {
    new FilteredChunkIterator(firstParent[T].iterator(split, context)
      , columnTypes, colIndex: Int, op: String, value: Int)
  }
}

class FilteredChunkIterator[T <: Product](itr: Iterator[T], columnTypes: Array[String]
                                          , colIndex: Int, op: String, value: Int)
  extends ChunkIterator[T](itr, columnTypes) {

  override def next(): RDDChunk[T] = {
    val chunk = new RDDChunk[T](columnTypes)
    chunk.fill(itr)
    chunk
  }
}

