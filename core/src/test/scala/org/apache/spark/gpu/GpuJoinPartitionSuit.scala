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

package org.apache.spark.gpu

import org.apache.spark.rdd.{GpuJoinPartition, GpuPartition}
import org.apache.spark.scheduler.OpenCLContext

import scala.language.existentials

/**
 *
 */
class GpuJoinPartitionSuit extends GpuSuit {

  val DEFAULT_CAPACITY = (1 << 10)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuJoinPartition(Int, Int) == 1 match test") {
    val leftTableData: IndexedSeq[(Int, Int)] = Array(10, 11, 10, 11, 12, 13, 13).zipWithIndex
    val leftPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    leftPartition.fill(leftTableData.toIterator)

    val rightTableData = Array(9, 10, 12, 13, 15).zipWithIndex
    val rightPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    rightPartition.fill(rightTableData.toIterator)

    val gpuJoinPartition = new GpuJoinPartition[(Int, Int, Int), (Int, Int), (Int, Int), Int](
      openCLContext, leftPartition, rightPartition, 0, 0, DEFAULT_CAPACITY)
    gpuJoinPartition.join

    val expectedData = Array((10, 0, 1), (10, 2, 1), (12, 4, 2), (13, 5, 3), (13, 6, 3))

    assert(rightPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((keyValue, leftValue, rightValue), index) =>
        assert(gpuJoinPartition.intData(0).get(index) === keyValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(1).get(index) === leftValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(2).get(index) === rightValue, f"values do not match at $index")
      case _ => fail("We should not be here")
    }
  }

  test("GpuJoinPartition(Int, Int) basic test") {
    val leftTableData = Array(15, 16, 17).zipWithIndex
    val leftPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    leftPartition.fill(leftTableData.toIterator)

    val rightTableData = Array(15, 16, 17).zipWithIndex
    val rightPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    rightPartition.fill(rightTableData.toIterator)

    val gpuJoinPartition = new GpuJoinPartition[(Int, Int, Int), (Int, Int), (Int, Int), Int](
      openCLContext, leftPartition, rightPartition, 0, 0, DEFAULT_CAPACITY)
    gpuJoinPartition.join

    val expectedData = Array((15, 0, 0), (16, 1, 1), (17, 2, 2))

    assert(gpuJoinPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((keyValue, leftValue, rightValue), index) =>
        assert(gpuJoinPartition.intData(0).get(index) === keyValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(1).get(index) === leftValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(2).get(index) === rightValue, f"values do not match at $index")
      case _ => fail("We should not be here")
    }
  }

  test("GpuJoinPartition(Long, Int) basic test") {
    val leftTableData: IndexedSeq[(Long, Int)] = Array(10L, 11L, 12L).zipWithIndex
    val leftPartition = new GpuPartition[(Long, Int)](openCLContext, DEFAULT_CAPACITY)
    leftPartition.fill(leftTableData.toIterator)

    val rightTableData = Array(10L, 11L, 12L).zipWithIndex
    val rightPartition = new GpuPartition[(Long, Int)](openCLContext, DEFAULT_CAPACITY)
    rightPartition.fill(rightTableData.toIterator)

    val gpuJoinPartition = new GpuJoinPartition[(Long, Int, Int), (Long, Int), (Long, Int), Long](
      openCLContext, leftPartition, rightPartition, 0, 0, DEFAULT_CAPACITY)
    gpuJoinPartition.join

    val expectedData = Array((10L, 0, 0), (11L, 1, 1), (12L, 2, 2))

    assert(gpuJoinPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((keyValue, leftValue, rightValue), index) =>
        assert(gpuJoinPartition.longData(0).get(index) === keyValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(0).get(index) === leftValue, f"values do not match at $index")
        assert(gpuJoinPartition.intData(1).get(index) === rightValue, f"values do not match at $index")
      case _ => fail("We should not be here")
    }
  }

  test("GpuJoinPartition(Long, Log) basic test") {
    val leftTableData = Array(10, 11, 12).zipWithIndex.map({ case (v, i) => (v.toLong, i.toLong) })
    val leftPartition = new GpuPartition[(Long, Long)](openCLContext, DEFAULT_CAPACITY)
    leftPartition.fill(leftTableData.toIterator)

    val rightTableData = Array(10, 11, 12).zipWithIndex.map({ case (v, i) => (v.toLong, i.toLong) })
    val rightPartition = new GpuPartition[(Long, Long)](openCLContext, DEFAULT_CAPACITY)
    rightPartition.fill(rightTableData.toIterator)

    val gpuJoinPartition = new GpuJoinPartition[(Long, Long, Long), (Long, Long), (Long, Long), Long](
      openCLContext, leftPartition, rightPartition, 0, 0, DEFAULT_CAPACITY)
    gpuJoinPartition.join

    val expectedData = Array((10L, 0L, 0L), (11L, 1L, 1L), (12L, 2L, 2L))

    assert(gpuJoinPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((keyValue, leftValue, rightValue), index) =>
        assert(gpuJoinPartition.longData(0).get(index) === keyValue, f"values do not match at $index")
        assert(gpuJoinPartition.longData(1).get(index) === leftValue, f"values do not match at $index")
        assert(gpuJoinPartition.longData(2).get(index) === rightValue, f"values do not match at $index")
      case _ => fail("We should not be here")
    }
  }

}
