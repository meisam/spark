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

import java.io._
import java.lang.reflect.{Array => JArray}

import org.apache.spark.{Logging, Partition, TaskContext}

import scala.reflect.ClassTag

/**
 *
 */
class GpuRDD[T <: Product : ClassTag](prev: RDD[T], val columnTypes: Array[String],
                                      chunkCapacity: Int)
  extends RDD[T](prev) {
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    new ChunkIterator(firstParent[T].iterator(split, context), columnTypes, chunkCapacity)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions
}

class RDDChunk[T <: Product : ClassTag](val columnTypes: Array[String], val capacity: Int)
  extends Serializable with Logging {

  def MAX_STRING_SIZE: Int = 1 << 7

  var size = 0

  val intData: Array[Array[Int]] = Array.ofDim[Int](columnTypes.filter(_ == "INT").length, capacity)
  val longData = Array.ofDim[Long](columnTypes.filter(_ == "LONG").length, capacity)
  val floatData = Array.ofDim[Float](columnTypes.filter(_ == "FLOAT").length, capacity)
  val doubleData = Array.ofDim[Double](columnTypes.filter(_ == "DOUBLE").length, capacity)
  val booleanData = Array.ofDim[Boolean](columnTypes.filter(_ == "BOOLEAN").length, capacity)
  val charData = Array.ofDim[Char](columnTypes.filter(_ == "CHAR").length, capacity)
  val stringData = Array.ofDim[Char](columnTypes.filter(_ == "STRING").length
    , capacity * MAX_STRING_SIZE)

  def fill(iter: Iterator[T]): Unit = {
    val values: Iterator[T] = iter.take(capacity)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        size = rowIndex
        v.productIterator.zipWithIndex.foreach { case (p, colIndex) =>
          if (columnTypes(colIndex) == "INT") {
            intData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Int]
          } else if (columnTypes(colIndex) == "LONG") {
            longData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Long]
          } else if (columnTypes(colIndex) == "FLOAT") {
            floatData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Float]
          } else if (columnTypes(colIndex) == "Double") {
            doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Double]
          } else if (columnTypes(colIndex) == "BOOLEAN") {
            booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Boolean]
          } else if (columnTypes(colIndex) == "CHAR") {
            charData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Char]
          } else if (columnTypes(colIndex) == "STRING") {
            val str = p.toString
            str.getChars(0, Math.min(MAX_STRING_SIZE, str.length),
              stringData(toTypeAwareColumnIndex(colIndex)), rowIndex * MAX_STRING_SIZE)
          }
        }
    }
  }

  def getStringData(typeAwareColumnIndex: Int, rowIndex: Int): String = {
    val str = new String(stringData(typeAwareColumnIndex), rowIndex * MAX_STRING_SIZE, MAX_STRING_SIZE)
    val actualLenght = str.indexOf(0)
    str.substring(0, actualLenght)
  }

  /**
   * Returns how many columns with the same type appear before the given column
   * in the underlying type of this chunk. For example, if the underlying type of the chunk is
   * (Int, Int, String, Int, String) the return values will be (0, 1, 0, 2, 1).
   *
   * @param columnIndex The given column index
   * @return Number of columns with the same type as the given column
   */
  def toTypeAwareColumnIndex(columnIndex: Int): Int = {
    val targetColumnType = columnTypes(columnIndex)
    val (taken, _) = columnTypes.splitAt(columnIndex)
    taken.filter(_ == targetColumnType).length
  }

  def apply(rowIndex: Int): T = {

    val values: Array[Any] = columnTypes.zipWithIndex.map({ case (colType, colIndex) =>
      println(colType, colIndex)
      if (colType == "INT") {
        intData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "LONG") {
        longData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "FLOAT") {
        floatData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "Double") {
        doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "BOOLEAN") {
        booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "CHAR") {
        charData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "STRING") {
        getStringData(toTypeAwareColumnIndex(colIndex), rowIndex)
      }
    })

    val resultTuple = values.length match {
      case 2 => (values(0), values(1)).asInstanceOf[T]
      case 3 => (values(0), values(1), values(2)).asInstanceOf[T]
      case 4 => (values(0), values(1), values(2), values(3)).asInstanceOf[T]
      case 5 => (values(0), values(1), values(2), values(3), values(4)).asInstanceOf[T]
      case 6 => (values(0), values(1), values(2), values(3), values(4), values(5)).asInstanceOf[T]
      case 7 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6)).asInstanceOf[T]
      case 8 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7)).asInstanceOf[T]
      case 9 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8)).asInstanceOf[T]
      case 10 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9)).asInstanceOf[T]
      case 11 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10)).asInstanceOf[T]
      case 12 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11)).asInstanceOf[T]
      case 13 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12)).asInstanceOf[T]
      case 14 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13)).asInstanceOf[T]
      case 15 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14)).asInstanceOf[T]
      case 16 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15)).asInstanceOf[T]
      case 17 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16)).asInstanceOf[T]
      case 18 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17)).asInstanceOf[T]
      case 19 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18)).asInstanceOf[T]
      case 20 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19)).asInstanceOf[T]
      case 21 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19), values(20)).asInstanceOf[T]
      case 22 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19), values(20), values(21)).asInstanceOf[T]
      case _ => throw new NotImplementedError("org.apache.spark.rdd.RDDChunk.apply is not " +
        "implemented yet")

    }
    resultTuple
  }
}

class ChunkIterator[T <: Product : ClassTag]
(itr: Iterator[T], val columnTypes: Array[String], chunkCapacity: Int = 1 << 20)
  extends Serializable with Iterator[T] {

  override def hasNext: Boolean = {
    currentPosition < chunkCapacity || itr.hasNext
  }

  private var currentPosition: Int = chunkCapacity

  private val currentChunk: RDDChunk[T] = new RDDChunk[T](columnTypes, chunkCapacity)

  override def next(): T = {
      format(currentPosition))
    if (currentPosition == chunkCapacity) {
      currentChunk.fill(itr)
      currentPosition = 0
    }
    val t: T = currentChunk(currentPosition)
    currentPosition += 1
    t
  }
}