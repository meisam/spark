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
import org.apache.spark.rdd.{ChunkIterator, RDDChunk}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials
import scala.reflect.ClassTag

/**
 *
 */
class GpuFilterRDDSuit extends FunSuite with SharedSparkContext {

  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  override def afterAll(): Unit = {
    // maybe release resources
  }

  test("kernel.genScanFilter_init_int_eq test") {
    val TEST_DATA_SIZE = 3 + (1 << 25) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val tupleNum: Long = testData.length
    val globalSize = tupleNum
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * tupleNum, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](tupleNum.toInt)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * globalSize, Pointer.to(resultData), 0, null, null)
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
    val TEST_DATA_SIZE = 3 + (1 << 25) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val tupleNum: Long = testData.length
    val globalSize = tupleNum
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * tupleNum, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_geq", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](tupleNum.toInt)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * globalSize, Pointer.to(resultData), 0, null, null)
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
