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
import org.apache.spark.rdd.{GpuJoinPartition, ComparisonOperation, GpuFilteredPartition, GpuPartition}
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */
class GpuJoinPartitionSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuJoinPartition(Int, Int) == 1 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val rightTableData = (1000 until 1000 + 10).zipWithIndex.filter(_._1 % 3 == 0)

    val gpuPartition = new GpuPartition[(Int, Int)](openCLContext, Array("INT", "INT"),
      DEFAULT_CAPACITY)
    gpuPartition.fill(rightTableData.toIterator)


    val gpuJoinPartition = new GpuJoinPartition[(Int, Int), (Int, Int), Int](openCLContext,
      Array("INT", "INT"), gpuPartition, 0, 0, DEFAULT_CAPACITY)

    gpuJoinPartition.fill(testData.toIterator)
    val expectedData = testData.filter(_._1 == 1000 + 1)

    assert(gpuPartition.size === expectedData.length)

    expectedData.foreach { case (value, index) =>
      assert(gpuPartition.intData(0)(index) === value, "values do not match")
      assert(gpuPartition.intData(1)(index) === index, "values do not match")
    case _ => fail("We should not be here")
    }
  }

}