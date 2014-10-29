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
import org.apache.spark.rdd.{ComparisonOperation, GpuPartition}
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

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
    // countScanNum does not do what I thought.
    val TEST_DATA_SIZE = 3 + (1 << 10)

    // the test sequence is     (0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,1,...)
    // the prefix sum should be (0,0,1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,...)
    val testData: Array[Int] = (0 until TEST_DATA_SIZE).map(_ % 3).map(_ % 2).toArray
    val tupleNum: Long = testData.length
    val globalSize = tupleNum / 4
    val localSize = Math.min(globalSize, 256)

    val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuFilter, CL_TRUE, 0, Sizeof.cl_int * tupleNum, Pointer.to(testData), 0, null, null)
    val gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    val kernel = clCreateKernel(openCLContext.getOpenCLProgram, "countScanNum", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    val resultData = new Array[Int](tupleNum.toInt)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuCount, CL_TRUE, 0, Sizeof.cl_int * globalSize, Pointer.to(resultData), 0, null, null)

    val expectedResults = (1 to TEST_DATA_SIZE).map(x => (1 + x) / 3)

    expectedResults.zip(resultData).zipWithIndex.foreach { case ((expected, actual), i) =>
      assert(expected === actual, "The %sths expected %,12d <> %,12d actual".format(i, expected, actual))
    }
  }

  test("kernel.my prefixSum test") {
    val TEST_DATA_SIZE = 3 + (1 << 14)

    // the test sequence is     (0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,1,0,0,...)
    // the prefix sum should be (0,1,1,1,2,2,2,3,3,3,4,4,4,5,5,5,6,6,6,...)
    val testData = (0 until TEST_DATA_SIZE).map(_ % 3).map(_ % 2).zipWithIndex

    val chunk = new GpuPartition[(Int, Int)](openCLContext, Array("INT", "INT"), DEFAULT_CAPACITY)
    chunk.fill(testData.toIterator)

    assert(chunk.intData(0) !== null)

    val expectedResults = (0 until TEST_DATA_SIZE).map(x => (2 + x) / 3).toArray
    val actualResults = new Array[Int](chunk.intData(0).length)

    chunk.prefixSum(chunk.intData(0), actualResults)

    assert(actualResults !== null)
    assert(actualResults.length !== expectedResults.length)
    expectedResults.zip(actualResults).zipWithIndex.foreach { case ((expected, actual), i) =>
      assert(expected === actual, "The %sths expected %,12d <> %,12d actual".format(i, expected, actual))
    }
  }

  test("kernel.my prefixSum small range test test") {
    val count = 10
    val testData = (0 until count).map(_ % 2).zipWithIndex

    val chunk = new GpuPartition[(Int, Int)](openCLContext, Array("INT", "INT"), DEFAULT_CAPACITY)

    chunk.fill(testData.toIterator)
    assert(chunk.intData(0) !== null)

    val expectedResults = Array(0, 1, 1, 2, 2, 3, 3, 4, 4, 5)
    val actualResults = new Array[Int](chunk.intData(0).length)
    chunk.prefixSum(chunk.intData(0), actualResults)

    assert(actualResults !== null)
    assert(actualResults.length !== expectedResults.length)
    expectedResults.zip(actualResults).zipWithIndex.foreach { case ((expected, actual), i) =>
      assert(expected === actual, "The %sths expected %,12d <> %,12d actual".format(i, expected, actual))
    }
  }

  test("kernel.my scan test") {
    val TEST_DATA_SIZE = 3 + (1 << 4)

    val count = 10
    val sourceCol = (0 until count).toArray
    val filter = sourceCol.map(x => (1 + x) % 2)

    val prefixSums = Array(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
    val resultSize = prefixSums(prefixSums.length - 1)
    val actualResults = Array.ofDim[Int](resultSize)

    val chunk = new GpuPartition[(Int, Int)](openCLContext,Array("INT", "INT"), DEFAULT_CAPACITY)
    chunk.fill(sourceCol.zipWithIndex.iterator)

    assert(chunk.intData(0) !== null)

    val expectedResults = (0 until count / 2).map(_ * 2).toArray
    chunk.scan(sourceCol, filter, prefixSums, actualResults, count)

    assert(actualResults !== null)
    assert(actualResults.length === expectedResults.length)

    expectedResults.zip(actualResults).zipWithIndex.foreach { case ((expected, actual), i) =>
      assert(expected === actual, "The %sths expected %,12d <> %,12d actual".format(i, expected, actual))
    }
  }

  test("org.apache.spark.rdd.GpuRDD.filter test") {
    // This crashes  the OpenCL device
    val testData: IndexedSeq[(Int, Int)] = (0 to 10).reverse.zipWithIndex

    val chunk = new GpuPartition[(Int, Int)](openCLContext, Array("INT", "INT"), DEFAULT_CAPACITY)
    chunk.fill(testData.toIterator)
    chunk.filter(1, 1, ComparisonOperation.<)

    (0 until chunk.capacity).foreach(i =>
      if (i <= 10) {
        assert(chunk.intData(0)(i) === (10 - i), "values do not match")
        assert(chunk.intData(1)(i) === i, "indexes  do not match")
      } else {
        assert(chunk.intData(0)(i) === 0, "values do not match")
        assert(chunk.intData(1)(i) === 0, "values do not match")
      }
    )
  }

      val rdd = sc.parallelize(testData)
      val gpuRdd = rdd.toGpuFilterRDD(Array("INT", "INT"), 0, 0, 1)
      val collectedChunks: Array[GpuPartition[Product]] = gpuRdd.collect()
      assert(collectedChunks.length === 1)
      val chunk = collectedChunks(0)
      assert(chunk.size === 1)
      assert(chunk.intData(0)(0) === 1, "values do not match")
      assert(chunk.intData(1)(1) === 1, "values do not match")
    }

  test("compute") {
    val testData: IndexedSeq[(Int, Int)] = (0 to 10).zipWithIndex

    val chunkIterator = new GpuFilteredPartitionIterator(testData.iterator, Array("INT", "INT"),
      openCLContext, 1, 0, 1)
    val column1 = (1 to 10).toArray
    val localSize = math.min(256, column1.length)
    val globalSize = localSize * math.min(1 + (column1.length - 1) / localSize, 2048)

    assert(globalSize === 10)
    val actualResult = chunkIterator.compute(column1, column1.length, 5, 3, globalSize, localSize)
    val expectedResult = column1.filter(_ < 5).length
    assert(actualResult === expectedResult)
  }

  test("org.apache.spark.rdd.GpuFilteredPartitionIterator.next time") {

    val size = 15
    val SIZE_OF_INTEGER = 4
    val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
    val value = 1

    val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0).toArray

    val iter = new GpuFilteredPartitionIterator[(Int, Int)](testData.zipWithIndex.iterator,
      Array("INT", "INT"), openCLContext, 0, 0, value)

    val localSize = math.min(256, testData.length)
    val globalSize = localSize * math.min(1 + (testData.length - 1) / localSize, 2048)
    iter.next()
  }

  test("org.apache.spark.rdd.GpuFilteredRDD full blown test") {
    val testData = (0 until 10).map(_ % 2).reverse.zipWithIndex.zipWithIndex.map(x => (x._1._1,
      x._1._2, x._2 * 5))

    val rdd = sc.parallelize(testData)
    println(rdd.collect().mkString(","))
    val filteredRDD = new GpuFilteredRDD(rdd, Array("INT", "INT", "INT"), 0, 0, 1)
    filteredRDD.foreach(chunk => {


      printf("Actual size= %,12d \n", chunk.size)
      chunk.intData.foreach(intArray => {
        println(intArray.take(chunk.size).mkString(","))
      })
    })
  }

}
