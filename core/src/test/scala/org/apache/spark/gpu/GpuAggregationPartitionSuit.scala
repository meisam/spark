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

import org.apache.spark.rdd.{AggregationExp, AggregationOperation, GpuAggregationPartition, GpuPartition, MathExp, MathOp, MathOperationType}
import org.apache.spark.scheduler.{OpenCLContextSingletone, OpenCLContext}

import scala.language.existentials

/**
 *
 */
class GpuAggregationPartitionSuit extends GpuSuit {

  val times2MathExo = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
  val const100Pi = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, 314)
  val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
  val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
  val countMathExp = new MathExp(MathOp.PLUS, 1, times2MathExo, null, MathOperationType.const, 15)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("Aggregation (Int, Int) result size test") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 3), (11, 1), (11, 4), (12, 6), (12, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Int), (Int, Int)](openCLContext,
      partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.sum, col1)) // sum col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData = Array((11, 2), (12, 6))

    assert(aggregationPartition.size === expectedData.length)
  }

  test("Aggregation sum(Int, Int) test") {
    val testData: IndexedSeq[(Int, Int)] = Array((111, 3), (111, 1), (111, 4), (112, 6), (112, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Float), (Int, Int)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.sum, col1)) // sum col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData: Array[(Int, Float)] = Array((111, 8f), (112, 11f))

    assert(aggregationPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((gbVal, aggValue), index) =>
        assert(aggregationPartition.intData(0).get(index) === gbVal, "values do not match")
        assert(aggregationPartition.floatData(0).get(index) === aggValue, "values do not match")
      case _ => fail("We should not be here")
    }
  }

  test("Aggregation sum(Int, Long)  size test") {
    val testData: IndexedSeq[(Int, Long)] = Array((11, 3L), (11, 1L), (11, 4L), (12, 6L), (12, 5L))

    val partition = new GpuPartition[(Int, Long)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Long), (Int, Long)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.sum, col1)) // sum col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData = Array((11, 4L), (12, 6L))

    assert(aggregationPartition.size === expectedData.length)
  }

  test("Aggregation count(Int, Int) test") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 3), (11, 1), (11, 4), (12, 6), (12, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Int), (Int, Int)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.count, col1)) // COUNT col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData = Array((12, 2), (11, 3))

    assert(aggregationPartition.size === expectedData.length)

    validateResults[(Int, Int)](expectedData, Array(aggregationPartition))
  }

  test("Aggregation MIN(Int, Float)  match test") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 3), (11, 1), (11, 4), (12, 6), (12, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Float), (Int, Int)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.min, col1)) // MIN col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData = Array((12, 5.0f), (11, 1.0f))

    validateResults[(Int, Float)](expectedData, Array(aggregationPartition))
  }

  test("Aggregation MAX(Int, Float)  match test") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 3), (11, 1), (11, 4), (12, 6), (12, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Float), (Int, Int)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, col0) // group by col 0
      , new AggregationExp(AggregationOperation.max, col1)) // MAX col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData = Array((12, 6.0f), (11, 4.0f))

    validateResults[(Int, Float)](expectedData, Array(aggregationPartition))
  }

  test("Aggregation sum(_, Int) all test") {
    val testData: IndexedSeq[(Int, Int)] = Array((111, 3), (111, 1), (111, 4), (112, 6), (112, 5))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val aggregationPartition = new GpuAggregationPartition[(Int, Float), (Int, Int)](openCLContext, partition,
      Array(new AggregationExp(AggregationOperation.groupBy, const100Pi) // group by all of them
        , new AggregationExp(AggregationOperation.sum, col1)) // sum col 1
      , DEFAULT_CAPACITY)

    aggregationPartition.aggregate()
    val expectedData: Array[(Int, Float)] = Array((const100Pi.opValue, 19f))

    assert(aggregationPartition.size === expectedData.length)

    expectedData.zipWithIndex.foreach {
      case ((gbVal, aggValue), index) =>
        assert(aggregationPartition.intData(0).get(index) === gbVal, "values do not match")
        assert(aggregationPartition.floatData(0).get(index) === aggValue, "values do not match")
      case _ => fail("We should not be here")
    }
  }


}