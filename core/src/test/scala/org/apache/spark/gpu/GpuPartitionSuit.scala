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
import org.apache.spark.rdd.GpuPartition
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

/**
 *
 */
class GpuPartitionSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }


  test("org.apache.spark.rdd.GpuPartition.initArray test") {
    val x = new GpuPartition[(Int, String, Float, Double, String)](openCLContext, DEFAULT_CAPACITY)
    assert(x.intData.length === 1)
    assert(x.longData.length === 0)
    assert(x.floatData.length === 1)
    assert(x.doubleData.length === 1)
    assert(x.stringData.length === 2)
  }

  test("org.apache.spark.rdd.GpuPartition.fill test") {
    val testData = (0 to 10).reverse.zipWithIndex.toIterator

    val chunk = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    chunk.fill(testData)
    (0 until chunk.capacity).foreach(i =>
      if (i <= 10) {
        assert(chunk.intData(0)(i) === (10 - i), "values do not match")
        assert(chunk.intData(1)(i) === i, "indexes  do not match")
      } else {
        assert(chunk.intData(0)(i) === 0, "values do not match")
        assert(chunk.intData(1)(i) === 0, "values do not match")
      }
    )
  }

  test("org.apache.spark.rdd.GpuPartition.toTypeAwareColumnIndex test") {
    val testData: IndexedSeq[(Int, String, Float, Double, String, Int, String)] = (0 to 10).map(x => (x, "STR_I_%d".format(x), 1.5f + x, 2.5d + x, "STR_II_%d".format(x), x - 1, "STR_III_%d".format(x)))

    val rddChunk = new GpuPartition[(Int, String, Float, Double, String, Int, String)](
      openCLContext, DEFAULT_CAPACITY)
    assert(rddChunk.toTypeAwareColumnIndex(0) === 0)
    assert(rddChunk.toTypeAwareColumnIndex(1) === 0)
    assert(rddChunk.toTypeAwareColumnIndex(2) === 0)
    assert(rddChunk.toTypeAwareColumnIndex(3) === 0)
    assert(rddChunk.toTypeAwareColumnIndex(4) === 1)
    assert(rddChunk.toTypeAwareColumnIndex(5) === 1)
    assert(rddChunk.toTypeAwareColumnIndex(6) === 2)
  }

  test("org.apache.spark.rdd.GpuPartition.getStringData test") {
    val testData: IndexedSeq[(String, String)] = (0 to 10).reverse.zipWithIndex.map(
      x => ("STR_I_%d".format(x._1), "STR_II_%d".format(x._2)))

    val chunk = new GpuPartition[(String, String)](openCLContext, DEFAULT_CAPACITY)
    chunk.fill(testData.toIterator)
    (0 until chunk.capacity).foreach(i =>
      if (i <= 10) {
        assert(chunk.getStringData(0, i) === ("STR_I_" + (10 - i)), "at row %d".format(i))
        assert(chunk.getStringData(1, i) === ("STR_II_" + i), "at row %d".format(i))
      } else {
        assert(chunk.getStringData(0, i) === "", "values do not match at row %d".format(i))
        assert(chunk.getStringData(1, i) === "", "values do not match at row %d".format(i))
      }
    )
  }

}