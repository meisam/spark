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
import org.apache.spark.rdd.{AggregationOperation, GpuAggregationPartition}
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */
class GpuAggregationPartitionSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuJoinPartition(Int, Int) == 1 match test") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 0), (11, 1), (11, 2), (12, 6), (12, 5))


    val gpuPartition = new GpuAggregationPartition[(Int, Int)](openCLContext
      , Array("INT", "INT"), Array(0), Array(AggregationOperation.max)
      , DEFAULT_CAPACITY)

    gpuPartition.fill(testData.toIterator)
    val expectedData = Array((11, 2), (12, 6))

    assert(gpuPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach { case ((gbVal, aggValue), index) =>
      assert(gpuPartition.intData(0)(index) === gbVal, "values do not match")
      assert(gpuPartition.intData(1)(index) === aggValue, "values do not match")
    case _ => fail("We should not be here")
    }
  }

}