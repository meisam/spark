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

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

/**
 *
 */

class GpuLayout extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)

  test("org.apache.spark.rdd.ChunkIterator test") {
    val testData = (0 to 10).reverse.zipWithIndex.toIterator

    val colTypes = Array("INT", "INT")
    val chunkItr = new ChunkIterator(testData, colTypes)

    val chunk = chunkItr.next
    (0 until chunk.MAX_SIZE).foreach(i =>
      if (i <= 10) {
        assert(chunk.intData(0)(i) === (10 - i), "values do not match")
        assert(chunk.intData(1)(i) === i, "indexes  do not match")
      } else {
        assert(chunk.intData(0)(i) === 0, "values do not match")
        assert(chunk.intData(1)(i) === 0, "values do not match")
      }
    )
  }

  test("GpuRDD(Int, Int) test") {
    val testData: IndexedSeq[(Int, Int)] = (0 to 10).reverse.zipWithIndex

    val rdd = sc.parallelize(testData)
    val gpuRdd = rdd.toGpuRDD(Array("INT", "INT"))
    val collectedChunks: Array[RDDChunk[Product]] = gpuRdd.collect()
    assert(collectedChunks.length === 1)
    val chunk = collectedChunks(0)
    (0 until chunk.MAX_SIZE).foreach(i =>
      if (i <= 10) {
        assert(chunk.intData(0)(i) === (10 - i), "values do not match")
        assert(chunk.intData(1)(i) === i, "indexes  do not match")
      } else {
        assert(chunk.intData(0)(i) === 0, "values do not match")
        assert(chunk.intData(1)(i) === 0, "values do not match")
      }
    )
  }

  test("GpuRDD(Int, String, Int, String) test") {
    val testData: IndexedSeq[(Int, String, Int, String)] = (0 to 10).reverse.zipWithIndex
      .map(x => (x._1, "STR_I_%d".format(x._1), x._2, "STR_II_%d".format(x._2)))

    val rdd = sc.parallelize(testData)
    val gpuRdd = rdd.toGpuRDD(Array("INT", "STRING", "INT", "STRING"))
    val collectedChunks: Array[RDDChunk[Product]] = gpuRdd.collect()
    assert(collectedChunks.length === 1)
    val chunk = collectedChunks(0)
    (0 until chunk.MAX_SIZE).foreach(i =>
      if (i <= 10) {
        assert(chunk.intData(0)(i) === (10 - i), "values do not match at row %d".format(i))
        assert(chunk.intData(1)(i) === i, "values do not match at row %d".format(i))
        assert(chunk.getStringData(0, i) === ("STR_I_" + (10 - i)), "at row %d".format(i))
        assert(chunk.getStringData(1, i) === ("STR_II_" + i), "at row %d".format(i))
      } else {
        assert(chunk.intData(0)(i) === 0, "values do not match")
        assert(chunk.intData(1)(i) === 0, "values do not match at row %d".format(i))
        assert(chunk.getStringData(0, i) === "", "values do not match at row %d".format(i))
        assert(chunk.getStringData(1, i) === "", "values do not match at row %d".format(i))
      }
    )
  }

  test("org.apache.spark.deploy.worker.WorkerArguments.inferDefaultGpu test") {
    val arguments = new WorkerArguments(Array("spark://localhost:7077"))
    assert(arguments.inferDefaultGpu() === 1, "There is one GPU on this device")
  }

}