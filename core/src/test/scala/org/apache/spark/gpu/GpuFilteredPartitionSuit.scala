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

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.{ComparisonOperation, GpuFilteredPartition}
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */
class GpuFilteredPartitionSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuFilteredPartition(Int, Int) == 1 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext,
      0, ComparisonOperation.==, 1000 + 1, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)
    val expectedData = testData.filter(_._1 == 1000 + 1)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) == 0 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext,
      0, ComparisonOperation.==, 20000, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)
    val expectedData = testData.filter(_._1 == 20000)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) == many matches test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).map(_ % 3).zipWithIndex

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext,
      0, ComparisonOperation.==, 2, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)
    val expectedData = testData.filter(_._1 == 2)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) >= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext,
      0, ComparisonOperation.>=, 7, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)

    val expectedData = testData.filter(_._1 >= 7)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) <= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext,
      0, ComparisonOperation.<=, 5, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)

    val expectedData = testData.filter(_._1 <= 5)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    }
  }

  test("GpuFilteredPartition(Long, Boolean) <= test") {
    val START = 1000
    val LENGHT = 10
    val testData: Array[(Long, Boolean)] = (START until START + LENGHT).zipWithIndex.map({ case (v, i) =>
      (v.toLong, ((i % 3) == 0))
    }).toArray

    val gpuPartition = new GpuFilteredPartition[(Long, Boolean), Long](openCLContext,
      0, ComparisonOperation.<=, START + 5L, DEFAULT_CAPACITY)
    gpuPartition.fill(testData.toIterator)

    val expectedData = testData.filter(_._1 <= START + 5)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach { case ((longValue, booleanValue), index) =>
      assert(gpuPartition.longData(0)(index) === longValue, "values do not match")
      assert(gpuPartition.booleanData(0)(index) === booleanValue, "values do not match")
    }
  }
}