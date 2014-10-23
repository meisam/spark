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
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */
class GpuRDDSuit extends FunSuite with SharedSparkContext {


  override def afterAll(): Unit = {
    // maybe release resources
  }

  test("org.apache.spark.rdd.GpuRDD 1 partition") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rdd = sc.parallelize(testData, PARTITIONS_COUNT)
    val gpuRDD = rdd.toGpuRDD(Array("INT", "INT"))
    val collectedData: Array[Product] = gpuRDD.collect
    assert(collectedData.length === testData.length)
    collectedData.zip(testData).foreach {
      case (v1, v2) => assert(v1 === v2)
    }
  }

  test("org.apache.spark.rdd.GpuRDD 2 partition") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rdd: RDD[(Int, Int)] = sc.parallelize(testData, PARTITIONS_COUNT)
    val gpuRDD = rdd.toGpuRDD(Array("INT", "INT"))
    val collectedData = gpuRDD.collect
    assert(collectedData.length === testData.length)
    collectedData.zip(testData).foreach {
      case (v1, v2) => assert(v1 === v2)
    }
    assert(gpuRDD.getPartitions.length === PARTITIONS_COUNT)
  }

  test("org.apache.spark.rdd.GpuRDD 2 partition and capacity < data size") {
    val PARTITIONS_COUNT = 3
    val TEST_DATA_SIZE = 3 + (1 << 10)
    val CHUNK_CAPACITY = 8
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rdd: RDD[(Int, Int)] = sc.parallelize(testData, PARTITIONS_COUNT)
    val gpuRDD = rdd.toGpuRDD(Array("INT", "INT"), CHUNK_CAPACITY)
    val collectedData = gpuRDD.collect
    assert(collectedData.length === testData.length)
    collectedData.zip(testData).foreach {
      case (v1, v2) => assert(v1 === v2)
    }
    assert(gpuRDD.getPartitions.length === PARTITIONS_COUNT)
  }

  test("org.apache.spark.rdd.GpuRDD with map afterwards") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val rdd = sc.parallelize(testData, PARTITIONS_COUNT)
    val gpuRDD: RDD[Int] = rdd.toGpuRDD(Array("INT", "INT")).map(x => x.productElement(0).asInstanceOf[Int])
    val collectedData = gpuRDD.collect
    val expectedData = testData.map(x => x.productElement(0).asInstanceOf[Int])
    assert(collectedData.length === expectedData.length)
    collectedData.zip(expectedData).foreach {
      case (v1, v2) => assert(v1 === v2)
    }
  }
}

