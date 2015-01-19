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
import org.apache.spark.rdd.{ ComparisonOperation, GpuFilteredPartition }
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{ Pointer, Sizeof }
import org.scalatest.FunSuite
import scala.collection.immutable.IndexedSeq
import scala.language.existentials
import org.apache.spark.rdd.GpuPartition

/**
 *
 */
class GpuFilteredRDDSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 20)
  val openCLContext = new OpenCLContext
  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("kernel.genScanFilter_init_int_eq test") {
    val TEST_DATA_SIZE = 3 + (1 << 10) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val globalSize = POW_2_S.filter(_ >= testData.length).head
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    clEnqueueWriteBuffer(openCLContext.queue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.program, "genScanFilter_init_int_eql", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](testData.length.toLong)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](testData.length.toInt)
    clEnqueueReadBuffer(openCLContext.queue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(resultData), 0, null, null)
    (0 until TEST_DATA_SIZE).foreach { i =>
      if (resultData(i) == 1) {
        assert(i === value)
        assert(testData(i) === value)
      } else
        assert(testData(i) === i)
    }
  }

  test("kernel.genScanFilter_init_int_geq test") {
    val TEST_DATA_SIZE = 3 + (1 << 10) // TODO larger data sizes may saturate java heap

    val testData: Array[Int] = (0 until TEST_DATA_SIZE).toArray
    val globalSize = POW_2_S.filter(_ >= testData.length).head
    val localSize = Math.min(globalSize, 256)
    val value = 50

    val gpuCol = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    clEnqueueWriteBuffer(openCLContext.queue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(testData), 0, null, null)

    val gpuFilter = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuPsum = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val gpuCount = clCreateBuffer(openCLContext.context, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = clCreateKernel(openCLContext.program, "genScanFilter_init_int_geq", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](globalSize)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](testData.length)
    clEnqueueReadBuffer(openCLContext.queue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(resultData), 0, null, null)
    (0 until TEST_DATA_SIZE).foreach { i =>
      if (resultData(i) == 1) {
        assert(testData(i) >= value)
      } else
        assert(testData(i) < value)
    }
  }

  test("org.apache.spark.rdd.GpuRDD.filter test") {
    val PARENT_SIZE = 10
    val testData: IndexedSeq[(Int, Int)] = (1 to PARENT_SIZE).reverse.zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    assert(parentPartition.size === PARENT_SIZE, "Size of the parent parition is incorrect")

    val COMPARISSION_VALUE = 3

    val filteredPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, parentPartition,
      1, ComparisonOperation.<, COMPARISSION_VALUE, DEFAULT_CAPACITY)
    filteredPartition.filter()

    (0 until filteredPartition.capacity).foreach(i =>
      if (i < COMPARISSION_VALUE) {
        assert(filteredPartition.intData(0).get(i) === (10 - i), "values do not match")
        assert(filteredPartition.intData(1).get(i) === i, "indexes  do not match")
      } else {
        assert(filteredPartition.intData(0).get(i) === 0, "values do not match")
        assert(filteredPartition.intData(1).get(i) === 0, "values do not match")
      })
  }
}
