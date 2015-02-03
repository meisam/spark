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

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}


class GpuRDD[
T <: Product : TypeTag
](
   @transient private var sc: SparkContext,
   @transient ar: Array[T], capacity: Int)
  extends RDD[GpuPartition[T]](sc, Nil) {
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * FIXME assuming there is only one partition
   */
  val allPartitions = Array[Partition](new ColumnarPartition(id, 0))

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = allPartitions

  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]] = {
    val partition = new GpuPartition[T](null, 0, capacity)
    println(ar)
    partition.fill(ar.toIterator)
    Array(partition).toIterator
  }
}

private class ColumnarPartition(rddId: Int, idx: Int)
  extends Partition {
  /**
   * Get the split's index within its parent RDD
   */
  override def index: Int = idx
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

  val columnTypes = typeOf[T] match {
    case ru.TypeRef(tpe, sym, typeArgs) => typeArgs
    case _ => throw new NotImplementedError("Unknown type %s".format(typeOf[T]))
  }

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

  val __partitions = Array(new GpuPartition[T](openCLContext, 0, capacity))

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = __partitions.asInstanceOf[Array[Partition]]

  /**
   * This field should be @transient because we want to initialize it after we send the task over
   * the network.
   */
  @transient var openCLContext: OpenCLContext = null

}