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

import org.apache.spark.scheduler.OpenCLContext
import org.apache.spark.util.NextIterator
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf, typeTag}
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
    new InterruptibleIterator[GpuPartition[T]]( context, new GpuPartitionIterator[T](data,
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
   paths: Array[String], capacity: Int)
  extends RDD[GpuPartition[T]](sc, Nil) {

  val rddId = sc.newRddId()

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {

    val partitionIterator = new NextIterator[GpuPartition[T]] {

      override def getNext() = {
        //assuming there is only one partition
        __partitions(0).fillFromFiles(paths)
        finished = true
        __partitions(0)
      }

      override def close(): Unit = {
        // TODO Close open streams?
      }

    }

    new InterruptibleIterator[GpuPartition[T]](context, partitionIterator)
  }

  val __partitions = Array(new GpuPartition[T](null, capacity))

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = __partitions.asInstanceOf[Array[Partition]]

}