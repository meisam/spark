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
import org.apache.spark.rdd.{ ComparisonOperation, GpuFilteredPartition }
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite
import scala.language.existentials
import org.apache.spark.rdd.GpuPartition

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

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)
    
    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      0, ComparisonOperation.==, 1000 + 1, DEFAULT_CAPACITY)
    gpuPartition.filter()

    val expectedData = testData.filter(_._1 == 1000 + 1)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((value1, value2), index) =>
        assert(gpuPartition.intData(0).get(index) === value1, "values do not match")
        assert(gpuPartition.intData(1).get(index) === value2, "values do not match")
      case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) == 0 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      0, ComparisonOperation.==, 20000, DEFAULT_CAPACITY)
    gpuPartition.filter()
    val expectedData = testData.filter(_._1 == 20000)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((value1, value2), index) =>
        assert(gpuPartition.intData(0).get(index) === value1, "values do not match")
        assert(gpuPartition.intData(1).get(index) === value2, "values do not match")
      case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) == many matches test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).map(_ % 3).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      0, ComparisonOperation.==, 2, DEFAULT_CAPACITY)
    gpuPartition.filter()
    val expectedData = testData.filter(_._1 == 2)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((value1, value2), index) =>
        assert(gpuPartition.intData(0).get(index) === value1, "values do not match")
        assert(gpuPartition.intData(1).get(index) === value2, "values do not match")
      case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) >= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      0, ComparisonOperation.>=, 7, DEFAULT_CAPACITY)
    gpuPartition.filter()

    val expectedData = testData.filter(_._1 >= 7)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((value1, value2), index) =>
        assert(gpuPartition.intData(0).get(index) === value1, "values do not match")
        assert(gpuPartition.intData(1).get(index) === value2, "values do not match")
      case _ => fail("We should not be here")
    }
  }

  test("GpuFilteredPartition(Int, Int) <= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      0, ComparisonOperation.<=, 5, DEFAULT_CAPACITY)
    gpuPartition.filter()

    val expectedData = testData.filter(_._1 <= 5)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((value1, value2), index) =>
        assert(gpuPartition.intData(0).get(index) === value1, "values do not match")
        assert(gpuPartition.intData(1).get(index) === value2, "values do not match")
    }
  }

  test("GpuFilteredPartition(Long, Boolean) <= test") {
    val START = 1000
    val LENGHT = 10
    val testData: Array[(Long, Boolean)] = (START until START + LENGHT).zipWithIndex.map({
      case (v, i) =>
        (v.toLong, ((i % 3) == 0))
    }).toArray

    val parentPartition = new GpuPartition[(Long, Boolean)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Long, Boolean), Long](openCLContext, parentPartition,
      0, ComparisonOperation.<=, START + 5L, DEFAULT_CAPACITY)
    gpuPartition.filter()

    val expectedData = testData.filter(_._1 <= START + 5)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((longValue, booleanValue), index) =>
        assert(gpuPartition.longData(0).get(index) === longValue, "values do not match")
        assert((gpuPartition.booleanData(0).get(index) != 0) === booleanValue, "values do not match")
    }
  }

  test("GpuFilteredPartition(Int, String) == test") {
    val START = 1000
    val LENGHT = 10
    val testData: Array[(Int, String)] = (START until START + LENGHT).map(v => (v, f"STR_VAL$v")).toArray

    val crieterion = "STR_VAL%d".format(START + 5)

    val parentPartition = new GpuPartition[(Int, String)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)
    
    val gpuPartition = new GpuFilteredPartition[(Int, String), String](openCLContext, parentPartition,
      0, ComparisonOperation.==, crieterion, DEFAULT_CAPACITY)
    gpuPartition.filter()

    val expectedData = testData.filter(_._2 == crieterion)

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((intValue, strValue), index) =>
        assert(gpuPartition.intData(0).get(index) === intValue, "values do not match")
        assert(gpuPartition.getStringData(0,index).equals(strValue), "values do not match")
    }
  }
}