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
import org.apache.spark.rdd.{GpuPartition, ComparisonOperation, GpuFilteredRDD}
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */
class GpuFilteredRDDSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)

  test("Filtered Partition (Int, Int)") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rdd = sc.parallelize(testData, PARTITIONS_COUNT)
    val gpuRDD = rdd.toGpuRDD[(Int, Int)]()


    val expectedDatea = testData.filter(_._1 == 10)
    val filteredData = new GpuFilteredRDD(gpuRDD, 0, ComparisonOperation.==, 10: Int,
      DEFAULT_CAPACITY)
    val collectedData: GpuPartition[(Int, Int)] = filteredData.collect()(0)
    assert(collectedData.size === expectedDatea.length)
    expectedDatea.zipWithIndex.foreach {
      case (v, i) =>
        assert(v._1 === collectedData.intData(0).get(i))
        assert(v._2 === collectedData.intData(1).get(i))
    }
  }

}
