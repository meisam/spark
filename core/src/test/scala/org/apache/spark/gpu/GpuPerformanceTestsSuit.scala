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

import java.util.concurrent.CountDownLatch

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.FilteredChunkIterator
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}
import org.scalatest.FunSuite

import scala.collection.immutable.IndexedSeq
import scala.language.existentials

/**
 * A set of test to measure the performance of GPU
 */
class GpuPerformanceTestsSuit extends FunSuite with SharedSparkContext {

  val openCLContext = new OpenCLContext
  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_DEBUGTRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  override def afterAll(): Unit = {
    // maybe release resources
  }

  ignore("selection with 10% selectivity transfer time test") {
    val SIZE_OF_INTEGER = 4
    (25 to 29).foreach { size => {
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0).toArray
      val globalSize = POW_2_S.filter(_ >= testData.length).head
      val localSize = Math.min(globalSize, 256)

      val gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)


      var totalTime = 0L

      (0 until 1000).foreach { x =>
        val startTime = System.nanoTime
        clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * testData.length, Pointer.to(testData), 0, null, null)
        val endTime = System.nanoTime
        totalTime += endTime - startTime
      }

      println("time to copy %,12d elements of data = %f".format(TEST_DATA_SIZE,
        totalTime.toDouble / 1000.0 / 1000000000))

      val gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
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
      //      (0 until TEST_DATA_SIZE).foreach { i =>
      //        if (resultData(i) == 1) {
      //          assert(i === value)
      //          assert(testData(i) === value)
      //        } else {
      //          assert(testData(i) === i)
      //        }
      //      }
    }
    }
  }

  ignore("selection with 10% selectivity on one CPU core") {
    val SIZE_OF_INTEGER = 4
    (20 to 29).foreach { size => {
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0)

      var totalTime = 0L
      val cores = 1

      val rdd = sc.parallelize(testData, cores)

      val iterations = 10
      (0 until iterations).foreach { x =>
        val startTime = System.nanoTime
        rdd.filter(_ == 1).collect()
        val endTime = System.nanoTime
        totalTime += endTime - startTime
      }

      println("time (ns) to do 10% selection on 1 CPU core %,12d integer elements of data = %f".format
        (TEST_DATA_SIZE, totalTime.toDouble / iterations))
    }
    }
  }

  ignore("selection with 10% selectivity scan") {
    val SIZE_OF_INTEGER = 4
    (26 until 27).foreach { size => {
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0).toArray


      val cores = 1

      val startTransformDataTime = System.nanoTime
      val iter = new FilteredChunkIterator[(Int, Int)](testData.zipWithIndex.iterator,
        Array("INT", "INT"), openCLContext, 0, 0, value)
      val endTransformDataTime = System.nanoTime

      val startSelectionTotalTime = System.nanoTime
      iter.selection(testData)
      val endSelectionTotalTime = System.nanoTime

      val totalTime = endSelectionTotalTime - startTransformDataTime
      println("Test with size=%,12d".format(size))
      println("Total transform time (ns) to copy %,12d elements of data = %,12d".format
        (TEST_DATA_SIZE, endTransformDataTime - startTransformDataTime))
      println("Selection time (ns) = %,12d".format
        (endSelectionTotalTime - startSelectionTotalTime))
      println("Total selection time (ns) = %,12d".format
        (totalTime))


    }
    }
  }

  test("two concurrent selection with 10% selectivity without data transformation") {
    val SIZE_OF_INTEGER = 4
    (25 until 26).foreach { size => {
      val TEST_DATA_SIZE = (1 << size) / SIZE_OF_INTEGER
      val selectivity = 10 //percent
      val value = 1

      val testData1 = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 0) value else 0).toArray
      val testData2 = (0 until TEST_DATA_SIZE).map(x => if (x % 10 == 1) value else 0).toArray

      // this is to avoid the huge cost of transforming data from row-store to columnar store
      val dummyData = (0 until 10).zipWithIndex.iterator

      val cores = 1

      val startTransformDataTime = System.nanoTime
      val iter = new FilteredChunkIterator[(Int, Int)](dummyData,
        Array("INT", "INT"), openCLContext, 0, 0, value)
      val endTransformDataTime = System.nanoTime

      val countDown = new CountDownLatch(2)

      val allData = Seq(testData1, testData1)
      val gpuRunnerThreads = allData.map(columnData => new GpuRunner(columnData, iter, countDown))

      val startSelectionTotalTime = System.nanoTime

      gpuRunnerThreads.foreach(_.start)

      countDown.await()

      val endSelectionTotalTime = System.nanoTime

      val totalTime = endSelectionTotalTime - startTransformDataTime
      println("Test with size=%,12d".format(size))
      println("Total transform time (ns) to copy %,12d elements of data = %,12d".format
        (TEST_DATA_SIZE, endTransformDataTime - startTransformDataTime))
      println("Selection time (ns) = %,12d".format
        (endSelectionTotalTime - startSelectionTotalTime))
      println("Total selection time (ns) = %,12d".format
        (totalTime))

    }
    }
  }
}

class GpuRunner(columnData: Array[Int]
                , iter: FilteredChunkIterator[(Int, Int)]
                , countDown: CountDownLatch) extends Thread {
  override def run(): Unit = {
    iter.nonBlockingSelection(columnData)
    countDown.countDown()
  }
}
