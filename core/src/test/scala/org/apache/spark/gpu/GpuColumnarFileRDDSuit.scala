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

import org.apache.spark.Logging
import org.apache.spark.rdd.{ComparisonOperation, GpuFilteredRDD, GpuRDD, GpuPartition}

import scala.language.existentials

/**
 *
 */
class GpuColumnarFileRDDSuit extends GpuSuit with Logging{

  test("org.apache.spark.rdd.GpuRDD 1 partition") {
    val PARTITIONS_COUNT = 1
    val CHUNK_CAPACITY = 1 << 10
    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE5"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE8")

    val dDateRaw: GpuRDD[(Int, Int, Int)] = sc.fromColumnarFiles[(Int, Int, Int)](ddatePaths, 1000,
      PARTITIONS_COUNT)
//    val dateResults = flattenResults[(Int,Int, Int)](dDateRaw.collect)
//    assert(dateResults.size === 2556)

    val filteredNyMonthRdd: GpuFilteredRDD[(Int, Int, Int), Int] = new GpuFilteredRDD[(Int, Int,
      Int), Int](dDateRaw, 1, ComparisonOperation.==, 199401, 50, NUMBER_OF_PARTITIONS)
//    val filteredNyMonthResults = flattenResults[(Int,Int, Int)](filteredNyMonthRdd.collect)
//    assert(filteredNyMonthResults.size === 31)

    val filteredByDayRdd: GpuFilteredRDD[(Int, Int, Int), Int] = new GpuFilteredRDD[(Int, Int,
      Int), Int](dDateRaw, 1, ComparisonOperation.==, 1, 50, NUMBER_OF_PARTITIONS)

    val filteredByDayResults = flattenResults[(Int,Int, Int)](filteredByDayRdd.collect)

    logInfo(f"testing version 8")
    assert(filteredByDayResults.size === 4)

  }
}

