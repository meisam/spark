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

import java.lang.reflect.{Array => JArray}

import org.apache.spark.{Logging, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 *
 */
class GpuRDD[T <: Product : ClassTag](prev: RDD[T], val columnTypes: Array[String])
  extends RDD[RDDChuck[T]](prev) {
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[RDDChuck[T]] = {
    new ChunkIterator(firstParent[T].iterator(split, context), columnTypes)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions

}

class RDDChuck[T <: Product](val columnTypes: Array[String]) extends Serializable with Logging {

  def MAX_SIZE: Int = 1 << 10

  def MAX_STRING_SIZE: Int = 1 << 7

  var actualSize = 0

  val intData: Array[Array[Int]] = Array.ofDim[Int](columnTypes.filter(_ == "INT").length, MAX_SIZE)
  val longData = Array.ofDim[Long](columnTypes.filter(_ == "LONG").length, MAX_SIZE)
  val floatData = Array.ofDim[Float](columnTypes.filter(_ == "FLOAT").length, MAX_SIZE)
  val doubleData = Array.ofDim[Double](columnTypes.filter(_ == "DOUBLE").length, MAX_SIZE)
  val booleanData = Array.ofDim[Boolean](columnTypes.filter(_ == "BOOLEAN").length, MAX_SIZE)
  val charData = Array.ofDim[Char](columnTypes.filter(_ == "CHAR").length, MAX_SIZE)
  val stringData = Array.ofDim[Char](columnTypes.filter(_ == "STRING").length
    , MAX_SIZE * MAX_STRING_SIZE)

  def fill(iter: Iterator[Product]): Unit = {
    val values: Iterator[Product] = iter.take(MAX_SIZE)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        actualSize = rowIndex
        v.productIterator.zipWithIndex.foreach { case (p, colIndex) =>
          if (columnTypes(colIndex) == "INT") {
            intData(colIndex)(rowIndex) = p.asInstanceOf[Int]
          } else if (columnTypes(colIndex) == "LONG") {
            longData(colIndex)(rowIndex) = p.asInstanceOf[Long]
          } else if (columnTypes(colIndex) == "FLOAT") {
            floatData(colIndex)(rowIndex) = p.asInstanceOf[Float]
          } else if (columnTypes(colIndex) == "Double") {
            doubleData(colIndex)(rowIndex) = p.asInstanceOf[Double]
          } else if (columnTypes(colIndex) == "BOOLEAN") {
            booleanData(colIndex)(rowIndex) = p.asInstanceOf[Boolean]
          } else if (columnTypes(colIndex) == "CHAR") {
            charData(colIndex)(rowIndex) = p.asInstanceOf[Char]
          } else if (columnTypes(colIndex) == "String") {
            p.asInstanceOf[String].getChars(0, MAX_STRING_SIZE, stringData(colIndex)
              , rowIndex * MAX_STRING_SIZE)
          }
        }
    }
  }

  def apply(i: Int): T = {
    throw new NotImplementedError("org.apache.spark.rdd.RDDChunk.apply is not implemented yet")

    /*
    genericType match {
    case _: Tuple1 => {
    val arr = rawData.asInstanceOf[Array[Array[Int]]]
    (arr(0)(i)).asInstanceOf[T]
    }
    case _: Tuple2 => (rawData(0)(i), rawData(1)(i)).asInstanceOf[T]
    case _: Tuple3 => (rawData(0)(i), rawData(1)(i), rawData(2)(i)).asInstanceOf[T]
    case _: Tuple4 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i))
      .asInstanceOf[T]
    case _: Tuple5 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i)).asInstanceOf[T]
    case _: Tuple6 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i)).asInstanceOf[T]
    case _: Tuple7 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i)).asInstanceOf[T]
    case _: Tuple8 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i)).asInstanceOf[T]
    case _: Tuple9 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i), rawData(9)(i)).asInstanceOf[T]
      case _ =>
    throw new IllegalArgumentException("%s is and unknown element type".format(classOf[T]))
    }
    */
  }

}

class ChunkIterator[T <: Product](itr: Iterator[T], val columnTypes: Array[String]) extends
Serializable with Iterator[RDDChuck[T]] {

  override def hasNext: Boolean = itr.hasNext

  override def next(): RDDChuck[T] = {
    val chunk = new RDDChuck[T](columnTypes)
    chunk.fill(itr)
    chunk
  }
}
