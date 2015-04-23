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

import org.apache.spark.rdd.{ComparisonOperation, GpuFilteredPartition, GpuPartition}
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}

import scala.language.existentials

/**
 *
 */
class GpuFilteredPartitionSuit extends GpuSuit {

  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuFilteredPartition(Int, Int) == 1 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    validateResults[(Int, Int)](testData.toArray, Array(parentPartition))

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      0, ComparisonOperation.==, 1000 + 1, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._1 == 1000 + 1)

    validateResults(expectedData.toArray, Array[GpuPartition[(Int, Int)]](gpuPartition))
  }

  test("GpuFilteredPartition(Int, Int) == 0 match test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      0, ComparisonOperation.==, 20000, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)
    val expectedData = testData.filter(_._1 == 20000)

    validateResults(expectedData.toArray, Array[GpuPartition[(Int, Int)]](gpuPartition))
  }

  test("GpuFilteredPartition(Int, Int) == many matches test") {
    val testData: IndexedSeq[(Int, Int)] = (1000 until 1000 + 10).map(_ % 3).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      0, ComparisonOperation.==, 2, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)
    val expectedData = testData.filter(_._1 == 2)

    validateResults(expectedData.toArray, Array[GpuPartition[(Int, Int)]](gpuPartition))
  }

  test("GpuFilteredPartition(Int, Int) >= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      0, ComparisonOperation.>=, 7, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._1 >= 7)

    validateResults(expectedData.toArray, Array[GpuPartition[(Int, Int)]](gpuPartition))
  }

  test("GpuFilteredPartition(Int, Int) <= test") {
    val testData: IndexedSeq[(Int, Int)] = (0 until 10).zipWithIndex

    val parentPartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      0, ComparisonOperation.<=, 5, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._1 <= 5)

    validateResults(expectedData.toArray, Array[GpuPartition[(Int, Int)]](gpuPartition))
  }

  test("GpuFilteredPartition(Long, Boolean) <= test") {
    val START = 1000
    val LENGHT = 10
    val testData: Array[(Long, Boolean)] = (START until START + LENGHT).zipWithIndex.map({
      case (v, i) =>
        (v.toLong, ((i % 3) == 0))
    }).toArray

    val parentPartition = new GpuPartition[(Long, Boolean)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Long, Boolean), Long](openCLContext, 0,
      0, ComparisonOperation.<=, START + 5L, DEFAULT_CAPACITY)
    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._1 <= START + 5)

    validateResults[(Long, Boolean)](expectedData.toArray, Array(gpuPartition))
  }

  test("GpuFilteredPartition(Int, String) == test") {
    val START = 1000
    val LENGTH = 10
    val testData: Array[(Int, String)] = (START until START + LENGTH).map(v => (v, f"STR_VAL$v")).toArray

    val criterion = "STR_VAL%d".format(START + 5)

    val parentPartition = new GpuPartition[(Int, String)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, String), String](openCLContext, 0,
      1, ComparisonOperation.==, criterion, DEFAULT_CAPACITY)

    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._2 == criterion)

    validateResults[(Int, String)](expectedData.toArray, Array(gpuPartition))
  }

  test("GpuFilteredPartition(Int, String) == more than local size test") {
    val START = 1000
    val LENGTH = 300
    val testData: Array[(Int, String)] = (START until START + LENGTH).map(v => (v, f"STR_VAL$v")).toArray

    val criterion = "STR_VAL%d".format(START + 5)

    val parentPartition = new GpuPartition[(Int, String)](openCLContext, DEFAULT_CAPACITY)
    parentPartition.fill(testData.toIterator)

    val gpuPartition = new GpuFilteredPartition[(Int, String), String](openCLContext, 0,
      1, ComparisonOperation.==, criterion, DEFAULT_CAPACITY)

    gpuPartition.filter(parentPartition)

    val expectedData = testData.filter(_._2 == criterion)

    validateResults[(Int, String)](expectedData.toArray, Array(gpuPartition))
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

    assert(parentPartition.size === PARENT_SIZE, "Size of the parent partition is incorrect")

    val COMPARISON_VALUE = 3

    val filteredPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 0,
      1, ComparisonOperation.<, COMPARISON_VALUE, DEFAULT_CAPACITY)
    filteredPartition.filter(parentPartition)

    val expectedData = testData.filter(_._2 < COMPARISON_VALUE).toArray

    validateResults[(Int, Int)](expectedData, Array(filteredPartition))

    (0 until filteredPartition.capacity).foreach(i =>
      if (i < COMPARISON_VALUE) {
        assert(filteredPartition.intData(0).get(i) === (10 - i), "values do not match")
        assert(filteredPartition.intData(1).get(i) === i, "indexes  do not match")
      } else {
        assert(filteredPartition.intData(0).get(i) === 0, "values do not match")
        assert(filteredPartition.intData(1).get(i) === 0, "values do not match")
      })
  }
}