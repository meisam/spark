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

import scala.language.existentials

/**
 * A set of test to measure the performance of GPU
 */
class CpuPerformanceTestsSuit extends GpuSuit {

  test("selection with 10% selectivity on one CPU core") {
    val SIZE_OF_INTEGER = 4
    (10 to 32).foreach { size =>
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0)

      var totalTime = 0L
      val cores = 1

      val rdd = sc.parallelize(testData, cores)

      val iterations = 10
      rdd.cache

      rdd.filter(_ == 1).count

      val filteredRdd = rdd.filter(_ == 1)
      for (x <- 0 to iterations){
        val startTime = System.nanoTime
        filteredRdd.collect()
        val endTime = System.nanoTime
        totalTime += endTime - startTime
      }

      println(f"${selectivity}d%% selection (1 CPU core) ${TEST_DATA_SIZE}%,d integer elements "
        + f" took ${totalTime / iterations}%,d nano seconds")
    }
  }
}
