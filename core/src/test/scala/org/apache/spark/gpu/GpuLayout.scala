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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.worker.WorkerArguments
import org.apache.spark.rdd.GpuPartitionIterator

import scala.language.existentials

/**
 *
 */

class GpuLayout extends GpuSuit {

  override val DEFAULT_CAPACITY = (1 << 5)

  // This test does not work with the new design of GpuPartition.
  test("org.apache.spark.rdd.GpuPartitionIterator test") {

    val testData = (0 to 1000).reverse.zipWithIndex.toArray

    val partitionItr = new GpuPartitionIterator(testData.toIterator, DEFAULT_CAPACITY)

    val partitions = partitionItr.toArray

    validateResults[(Int, Int)](testData, partitions)
  }

  test("org.apache.spark.deploy.worker.WorkerArguments.inferDefaultGpu test") {
    val conf = new SparkConf(true)
    val arguments = new WorkerArguments(Array("spark://localhost:7077"), conf)
    assert(arguments.inferDefaultGpu() === 1, "There is one GPU on this device")
  }

}