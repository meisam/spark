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
import org.apache.spark.rdd.FilteredChunkIterator
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

/**
 *
 */
class GpuFilterRDDSuit extends FunSuite with SharedSparkContext {

  val openCLContext = new OpenCLContext
  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  override def afterAll(): Unit = {
    // maybe release resources
  }

  test("kernel.genScanFilter_init_int_eq test") {
    val TEST_DATA_SIZE = 3 + (1 << 10) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val globalSize = POW_2_S.filter(_ >= testData.length).head
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](testData.length.toLong)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](testData.length.toInt)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(resultData), 0, null, null)
    (0 until TEST_DATA_SIZE).foreach { i =>
      if (resultData(i) == 1) {
        assert(i === value)
        assert(testData(i) === value)
      }
      else
        assert(testData(i) === i)
    }
  }

  test("kernel.genScanFilter_init_int_geq test") {
    val TEST_DATA_SIZE = 3 + (1 << 10) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val globalSize = POW_2_S.filter(_ >= testData.length).head
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_geq", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](globalSize)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](testData.length)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(resultData), 0, null, null)
    (0 until TEST_DATA_SIZE).foreach { i =>
      if (resultData(i) == 1) {
        assert(testData(i) >= value)
      }
      else
        assert(testData(i) < value)
    }
  }

  test("kernel.countScanNum test") {
  test("org.apache.spark.rdd.GpuRDD.filter test") {
  test("kernel.my prefixSum test") {
    val TEST_DATA_SIZE = 3 + (1 << 4)
    println("TEST_DATA_SIZE=%d".format(TEST_DATA_SIZE))

    // the test sequence is     (0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,...)
    // the prefix sum should be (0,1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,...)
    val testData = (0 until TEST_DATA_SIZE).map(_ % 3).map(_ % 2).zipWithIndex

    val iter = new FilteredChunkIterator[(Int, Int)](testData.iterator, Array("INT", "INT"), openCLContext, 0, 0, 1)
    assert(iter.hasNext)
    val chunk = iter.next()
    assert(!iter.hasNext)

    assert(chunk.intData(0) !== null)
    assert(chunk.intData(0).length === chunk.MAX_SIZE)

    val expectedResults = (0 until TEST_DATA_SIZE).map(x => (2 + x) / 3).toArray
    val actualResults = new Array[Int](chunk.intData(0).length)
    iter.prefixSum(chunk.intData(0), actualResults)

    assert(actualResults !== null)
    assert(actualResults.length !== expectedResults.length)
    expectedResults.zip(actualResults).zipWithIndex.foreach { case ((expected, actual), i) =>
      assert(expected === actual, "The %sths expected %d <> %d actual".format(i, expected, actual))
    }
  }

  ignore("org.apache.spark.rdd.GpuRDD.filter test") {
    val testData: IndexedSeq[(Int, Int)] = (0 to 10).reverse.zipWithIndex

    val rdd = sc.parallelize(testData)
    val gpuRdd = rdd.toGpuFilterRDD(Array("INT", "INT"), 1, 0, 1)
    val collectedChunks = gpuRdd.collect()
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

}
