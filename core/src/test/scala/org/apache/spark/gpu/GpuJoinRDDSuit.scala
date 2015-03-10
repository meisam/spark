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

import org.apache.spark.rdd.{GpuJoinRDD, ComparisonOperation, GpuFilteredRDD}

import scala.language.existentials

/**
 *
 */
class GpuJoinRDDSuit extends GpuSuit {

  test("Join Rdd (Int, Int), (Int, Int) => (Int, Int, Int)") {
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val leftData: Array[(Int, Int)] = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rightData: Array[(Int, Int)] = (0 until TEST_DATA_SIZE).zipWithIndex.filter(_._2 % 2 == 0).toArray
    val leftRDD = sc.toGpuRDD[(Int, Int)](leftData)
    val rightRDD = sc.toGpuRDD[(Int, Int)](rightData)

    val expectedData = leftData.zipWithIndex.filter(p => p._2 % 2 == 0).map( x => (x._1._1, x._1
      ._2, x._2))
    val joinData = new GpuJoinRDD[(Int, Int, Int), (Int, Int), (Int, Int), Int](leftRDD, rightRDD,
      1, 1, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val collectedPartitions = joinData.collect()
    validateResults(expectedData, collectedPartitions)
  }

  test("Join Rdd (Int, Int), (Int, Int) => (Int, Int, Int) low capacity") {
    val TEST_DATA_SIZE = 3 + (1 << 10)
    val leftData: Array[(Int, Int)] = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rightData: Array[(Int, Int)] = (0 until TEST_DATA_SIZE).zipWithIndex.filter(_._2 % 2 == 0).toArray
    val leftRDD = sc.toGpuRDD[(Int, Int)](leftData)
    val rightRDD = sc.toGpuRDD[(Int, Int)](rightData)

    val expectedData = leftData.zipWithIndex.filter(p => p._2 % 2 == 0).map( x => (x._1._1, x._1
      ._2, x._2))
    val joinData = new GpuJoinRDD[(Int, Int, Int), (Int, Int), (Int, Int), Int](leftRDD, rightRDD,
      1, 1, 500, NUMBER_OF_PARTITIONS)

    val collectedPartitions = joinData.collect()

    println(collectedPartitions.length)
    validateResults(expectedData, collectedPartitions)
  }

}
