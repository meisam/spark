/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.reflect.runtime.{universe => ru}


class GpuRDD[
T <: Product : TypeTag
](
   @transient private var sc: SparkContext,
   @transient data: Array[T], capacity: Int, numPartitions: Int)
  extends RDD[GpuPartition[T]](sc, Nil) {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {
    implicit val ct = ClassTag[T](typeTag[T].mirror.runtimeClass(typeTag[T].tpe))
    val slices = ParallelCollectionRDD.slice(data, numPartitions)(ct).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(p: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {
    val data: Iterator[T] = p.asInstanceOf[ParallelCollectionPartition[T]].iterator
    new InterruptibleIterator[GpuPartition[T]](context, new GpuPartitionIterator[T](data,
      capacity))
  }
}

/**
 *
 */
class ColumnarFileGpuRDD[
T <: Product : TypeTag
](
   @transient private var sc: SparkContext,
   paths: Array[String], capacity: Int, numPartitions: Int)
  extends RDD[GpuPartition[T]](sc, Nil) {

  val rddId = sc.newRddId()

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {

    val partitionDescriptor = partition.asInstanceOf[ParquetPartitionDescriptor]
    val sliceId = partitionDescriptor.index
    val sliceCount = partitionDescriptor.sliceCount
    val paths = partitionDescriptor.paths

    val gpuPartition = new GpuPartition[T](context.getOpenCLContext, capacity)
    val elementsCount = gpuPartition.getSizeFromFile(paths.head)

    val partitionCount = Math.ceil(elementsCount / capacity)

    var fromIndex = 0

    val itr = new NextIterator[GpuPartition[T]] {/**
     * Method for subclasses to implement to provide the next element.
     *
     * If no next element is available, the subclass should set `finished`
     * to `true` and may return any value (it will be ignored).
     *
     * This convention is required because `null` may be a valid value,
     * and using `Option` seems like it might create unnecessary Some/None
     * instances, given some iterators might be called in a tight loop.
     *
     * @return U, or set 'finished' when done
     */
    override protected def getNext(): GpuPartition[T] = {
      gpuPartition.size = 0
      gpuPartition.fillFromFiles(paths, fromIndex)
      fromIndex = fromIndex + capacity
      finished = (fromIndex >= elementsCount)
      logInfo(f"Finished partitioning data? ${finished} ${fromIndex}/${elementsCount} elements")
      gpuPartition
    }

      /**
       * Method for subclasses to implement when all elements have been successfully
       * iterated, and the iteration is done.
       *
       * <b>Note:</b> `NextIterator` cannot guarantee that `close` will be
       * called because it has no control over what happens when an exception
       * happens in the user code that is calling hasNext/next.
       *
       * Ideally you should have another try/catch, as in HadoopRDD, that
       * ensures any resources are closed should iteration fail.
       */
      override protected def close(): Unit = {

      }
    }

    new InterruptibleIterator[GpuPartition[T]](context, itr)
  }


  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {

    (0 until numPartitions).toArray.map {
      i => new ParquetPartitionDescriptor(id, i, paths)
    }
  }
}

class ParquetPartitionDescriptor(id: Int, val sliceCount: Int, val paths: Array[String])
  extends Partition {
  /**
   * Get the split's index within its parent RDD
   */
  override def index: Int = id
}