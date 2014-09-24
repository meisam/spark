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

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 *
 */
class GpuRDD[T](prev: RDD[T]) extends RDD[RDDChuck[T]](prev) {
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[RDDChuck[T]] = {

  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions

}

class RDDChuck[T: ClassTag] {

  def size: Int = (1 << 20)

  var rawData: Array[Array[Any]] = initArray

  def initArray(): Array[Array[Any]] = {
    classOf[T] match {
      case t: Tuple1 => Array.ofDim[Int](size, 1).asInstanceOf[Array[Array[Any]]]
      case t: Tuple2 => Array.ofDim[](size, 2)
    }
  }

  def apply(i: Int): T = {
    classOf[T] match {
      case _: Tuple1 => (rawData(0)(i)).asInstanceOf[T]
      case _: Tuple2 => (rawData(0)(i), rawData(1)(i)).asInstanceOf[T]
      case _: Tuple3 => (rawData(0)(i), rawData(1)(i), rawData(2)(i)).asInstanceOf[T]
      case _: Tuple4 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i)).asInstanceOf[T]
      case _: Tuple5 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i), rawData(4)(i)).asInstanceOf[T]
      case _: Tuple6 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i), rawData(4)(i), rawData(5)(i)).asInstanceOf[T]
      case _: Tuple7 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i), rawData(4)(i), rawData(5)(i), rawData(6)(i)).asInstanceOf[T]
      case _: Tuple8 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i), rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i)).asInstanceOf[T]
      case _: Tuple9 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i), rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i), rawData(9)(i)).asInstanceOf[T]
      case _ =>
        throw new IllegalArgumentException("%s is and unknown element type".format(classOf[T]))
    }
  }

}
