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
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

/**
 * A set of test to measure the performance of GPU
 */
class CpuPerformanceTestsSuit extends FunSuite with SharedSparkContext {

  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)


  test("selection with 10% selectivity on one CPU core") {
    val SIZE_OF_INTEGER = 4
    (10 to 12).foreach { size => {
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0)

      var totalTime = 0L
      val cores = 1

      val rdd = sc.parallelize(testData, cores)

      val iterations = 1000
      (0 until iterations).foreach { x =>
        val startTime = System.nanoTime
        rdd.filter(_ == 1).collect()
        val endTime = System.nanoTime
        totalTime += endTime - startTime
      }

      println("time (ns) to do 10% selection on 1 CPU core %d bytes of data = %f".format
        (TEST_DATA_SIZE, totalTime.toDouble / iterations))
    }
    }
  }
}
