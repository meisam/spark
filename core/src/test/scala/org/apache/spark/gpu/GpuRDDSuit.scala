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
import org.scalatest.FunSuite

import scala.language.existentials
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 *
 */
class GpuRDDSuit extends FunSuite with SharedSparkContext {

  private def flattenResults[T <: Product : TypeTag : ClassTag](collectedPartitions: Array[GpuPartition[T]]): Array[T] = {

    val collectedData: Array[T] = collectedPartitions.map { p =>
      (0 until p.size).map(i => p(i))
    }.flatten[T]
    collectedData
  }

  def validateResults[T <: Product : TypeTag : ClassTag](testData: Array[T], collectedPartitions: Array[GpuPartition[T]]):
  Unit = {
    val collectedData = flattenResults[T](collectedPartitions)
    testData.zip(collectedData).foreach {
      case (vt, vc) =>
        assert(vt === vc)
    }

    val totalResult = collectedPartitions.map(p => p.size).reduce(_ + _)
    assert(totalResult === testData.length)
  }

  test("org.apache.spark.rdd.GpuRDD 1 partition") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val CHUNK_CAPACITY = 1 << 10
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val gpuRDD = sc.toGpuRDD[(Int, Int)](testData, CHUNK_CAPACITY, PARTITIONS_COUNT)
    val collectedPartitions = gpuRDD.collect()
    assert(collectedPartitions.size === PARTITIONS_COUNT)
    validateResults(testData, collectedPartitions)
  }

  test("org.apache.spark.rdd.GpuRDD 2 partition") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val CHUNK_CAPACITY = 1 << 10
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val gpuRDD = sc.toGpuRDD[(Int, Int)](testData, CHUNK_CAPACITY, PARTITIONS_COUNT)
    val collectedPartitions = gpuRDD.collect()
    assert(collectedPartitions.length === PARTITIONS_COUNT)

    validateResults(testData, collectedPartitions)
  }


  test("org.apache.spark.rdd.GpuRDD 3 partition and capacity < data size") {
    val PARTITIONS_COUNT = 3
    val TEST_DATA_SIZE = 3 + (1 << 10)
    val CHUNK_CAPACITY = 8
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val gpuRDD = sc.toGpuRDD[(Int, Int)](testData, CHUNK_CAPACITY, PARTITIONS_COUNT)
    val collectedPartitions: Array[GpuPartition[(Int, Int)]] = gpuRDD.collect()
    assert(collectedPartitions.length === Math.ceil(TEST_DATA_SIZE.toDouble / CHUNK_CAPACITY))
    validateResults(testData, collectedPartitions)

  }

  // This would not work with the given design of GpuRDD
  ignore("org.apache.spark.rdd.GpuRDD with map afterwards") {
    val PARTITIONS_COUNT = 1
    val TEST_DATA_SIZE = 3 + (1 << 4)
    val testData = (0 until TEST_DATA_SIZE).reverse.zipWithIndex.toArray
    val gpuRDD = sc.toGpuRDD[(Int, Int)](testData) // FIXME.map(_._1)
    val collectedPartitions = gpuRDD.collect()
    val expectedData = testData.map(x => x.productElement(0).asInstanceOf[Int])
    assert(collectedPartitions.size === PARTITIONS_COUNT)
    validateResults(testData, collectedPartitions)
  }


  test("GpuRDD(Int, Int) test") {
    val PARTITIONS_COUNT = 1

    val testData = (0 to 10).reverse.zipWithIndex.toArray

    val gpuRdd = sc.toGpuRDD[(Int, Int)](testData)
    val collectedPartitions = gpuRdd.collect()
    val expectedData = testData.map(x => x.productElement(0).asInstanceOf[Int])
    assert(collectedPartitions.size === PARTITIONS_COUNT)
    validateResults(testData, collectedPartitions)
  }

  test("GpuRDD(Int, String, Int, String) test") {
    val testData = (0 to 10).reverse.zipWithIndex.map { x =>
      (x._1, "STR_I_%d".format(x._1), x._2, "STR_II_%d".format(x._2))
    }.toArray

    val gpuRdd = sc.toGpuRDD[(Int, String, Int, String)](testData)
    val collectedData = gpuRdd.collect()(0)
    assert(collectedData.size === testData.length)
    testData.zipWithIndex.foreach { case (v, i) =>
      val v1 = collectedData.intData(0).get(i)
      val v2 = collectedData.stringData(0).get(i)
      val v3 = collectedData.intData(1).get(i)
      val v4 = collectedData.stringData(1).get(i)
      if (i <= 10) {
        assert(v1 === (10 - i), "values do not match at row %d".format(i))
        assert(v2 === ("STR_I_" + (10 - i)), "at row %d".format(i))
        assert(v3 === i, "values do not match at row %d".format(i))
        assert(v4 === ("STR_II_" + i), "at row %d".format(i))
      } else {
        assert(v1 === 0, "values do not match")
        assert(v2 === "", "values do not match at row %d".format(i))
        assert(v3 === 0, "values do not match at row %d".format(i))
        assert(v4 === "", "values do not match at row %d".format(i))
      }
    }
  }
}

