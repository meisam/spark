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
import org.apache.spark.deploy.worker.WorkerArguments
import org.apache.spark.rdd.ChunkIterator
import org.scalatest.FunSuite

import scala.language.existentials

/**
 *
 */

class GpuLayout extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)

  test("org.apache.spark.rdd.ChunkIterator test") {
    val testData = (0 to 10).reverse.zipWithIndex.toIterator

    val colTypes = Array("INT", "INT")
    val chunkItr = new ChunkIterator(testData, colTypes, DEFAULT_CAPACITY)

    chunkItr.zipWithIndex.foreach {
      case ((v1, v2), i) =>
        if (i <= 10) {
          assert(v1 === (10 - i), "values do not match")
          assert(v2 === i, "indexes  do not match")
        } else {
          assert(v1 === 0, "values do not match")
          assert(v2 === 0, "values do not match")
        }
    }
  }

  test("org.apache.spark.deploy.worker.WorkerArguments.inferDefaultGpu test") {
    val arguments = new WorkerArguments(Array("spark://localhost:7077"))
    assert(arguments.inferDefaultGpu() === 1, "There is one GPU on this device")
  }

}