package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.apache.spark.{Partition, TaskContext}
import org.jocl.CL._
import org.jocl._

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag


/**
 * Created by fathi on 10/1/14.
 */
class GpuFilteredRDD[T <: Product : ClassTag]
(prev: RDD[T], val columnTypes: Array[String]
 , colIndex: Int, operation: Int, value: Int)
  extends RDD[RDDChunk[T]](prev) {

  override def getPartitions: Array[Partition] = firstParent[RDDChunk[T]].partitions

  override val partitioner = prev.partitioner // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext): FilteredChunkIterator[T] = {
    new FilteredChunkIterator(firstParent[T].iterator(split, context)
      , columnTypes, openCLContext, colIndex, operation, value)
  }
}

class FilteredChunkIterator[T <: Product]
(itr: Iterator[T], columnTypes: Array[String], var openCLContext: OpenCLContext, colIndex: Int, operation: Int, value: Int)
  extends Iterator[RDDChunk[T]] {

  def isPowerOfTwo(n: Int): Boolean = {
    return ((n & (n - 1)) == 0)
  }

  def floorPow2(n: Int): Int = {
    var cal: Int = 1
    var exp: Int = 0
    while (cal <= n) {
      cal *= 2
      exp += 1
    }
    return 1 << (exp - 1)
  }

  def preallocBlockSums(maxNumElements: Int, context: OpenCLContext) {
    g_numEltsAllocated = maxNumElements
    val blockSize: Int = BLOCK_SIZE
    var numElts: Int = maxNumElements
    var level: Int = 0
    do {
      val numBlocks: Int = Math.max(1,
        Math.ceil(numElts.asInstanceOf[Int] / (2.0f * blockSize)).asInstanceOf[Int])
      if (numBlocks > 1) {
        level += 1
      }
      numElts = numBlocks
    } while (numElts > 1)
    g_scanBlockSums = new Array[cl_mem](level)
    g_numLevelsAllocated = level
    numElts = maxNumElements
    level = 0
    do {
      val numBlocks: Int = Math.max(1,
        Math.ceil(numElts.asInstanceOf[Int] / (2.0f * blockSize)).asInstanceOf[Int])
      if (numBlocks > 1) {
        g_scanBlockSums(level) = clCreateBuffer(context.context, CL_MEM_READ_WRITE, numBlocks * Sizeof.cl_int, null, null)
        level += 1
      }
      numElts = numBlocks
    } while (numElts > 1)
  }

  def prescanArrayRecursive(outArray: cl_mem, inArray: cl_mem, numElements: Int, level: Int, same: Int, context: OpenCLContext) {


    val blockSize: Int = BLOCK_SIZE
    val waitEvents = Array(new cl_event)

    val numBlocks: Int = Math.max(1, Math.ceil(numElements.asInstanceOf[Int] / (2.0f * blockSize)).asInstanceOf[Int])
    var numThreads: Int = 0
    var kernel: cl_kernel = null
    if (numBlocks > 1) numThreads = blockSize
    else if (isPowerOfTwo(numElements)) numThreads = numElements / 2
    else numThreads = floorPow2(numElements)
    val numEltsPerBlock: Int = numThreads * 2
    val numEltsLastBlock: Int = numElements - (numBlocks - 1) * numEltsPerBlock
    var numThreadsLastBlock: Int = Math.max(1, numEltsLastBlock / 2)
    var np2LastBlock: Int = 0
    var sharedMemLastBlock: Int = 0
    if (numEltsLastBlock != numEltsPerBlock) {
      np2LastBlock = 1
      if (!isPowerOfTwo(numEltsLastBlock)) numThreadsLastBlock = floorPow2(numEltsLastBlock)
      val extraSpace: Int = (2 * numThreadsLastBlock) / NUM_BANKS
      sharedMemLastBlock = Sizeof.cl_int * (2 * numThreadsLastBlock + extraSpace)
    }
    val extraSpace: Int = numEltsPerBlock / NUM_BANKS
    val sharedMemSize: Int = Sizeof.cl_int * (numEltsPerBlock + extraSpace)
    var localSize: Long = numThreads
    var globalSize: Long = Math.max(1, numBlocks - np2LastBlock) * localSize
    var tmp: Int = 0
    if (numBlocks > 1) {
      kernel = clCreateKernel(context.program, "prescan", null)
      clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
      if (same == 0) clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(inArray))
      else {
        val tmpP: cl_mem = clCreateBuffer(context.context, CL_MEM_READ_WRITE, Sizeof.cl_int, null, null)
        clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(tmpP))
      }
      clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(g_scanBlockSums(level)))
      tmp = numThreads * 2
      clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 0
      clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 5, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 1
      clSetKernelArg(kernel, 6, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 0
      clSetKernelArg(kernel, 7, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 8, Sizeof.cl_int, Pointer.to(Array[Int](same)))
      clSetKernelArg(kernel, 9, sharedMemSize, null)
      var global_work_size = Array[Long](1)
      global_work_size(0) = globalSize
      var local_work_size = Array[Long](1)
      local_work_size(0) = localSize
      clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      if (np2LastBlock != 0) {
        kernel = clCreateKernel(context.program, "prescan", null)
        clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
        if (same == 0) clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(inArray))
        else {
          val tmpP: cl_mem = clCreateBuffer(context.context, CL_MEM_READ_WRITE, Sizeof.cl_int, null, null)
          clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(tmpP))
        }
        clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(g_scanBlockSums(level)))
        clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](numEltsLastBlock)))
        tmp = numBlocks - 1
        clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        tmp = numElements - numEltsLastBlock
        clSetKernelArg(kernel, 5, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        tmp = 1
        clSetKernelArg(kernel, 6, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        clSetKernelArg(kernel, 7, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        clSetKernelArg(kernel, 8, Sizeof.cl_int, Pointer.to(Array[Int](same)))
        clSetKernelArg(kernel, 9, sharedMemLastBlock, null)
        localSize = numThreadsLastBlock
        globalSize = numThreadsLastBlock
        global_work_size = Array[Long](globalSize)
        local_work_size = Array[Long](localSize)
        clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      }
      prescanArrayRecursive(g_scanBlockSums(level), g_scanBlockSums(level), numBlocks, level + 1, 1, context)
      kernel = clCreateKernel(context.program, "uniformAdd", null)
      clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
      clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(g_scanBlockSums(level)))
      tmp = numElements - numEltsLastBlock
      clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 0
      clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      localSize = numThreads
      globalSize = Math.max(1, numBlocks - np2LastBlock) * localSize
      clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      if (np2LastBlock != 0) {
        kernel = clCreateKernel(context.program, "uniformAdd", null)
        clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
        clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(g_scanBlockSums(level)))
        clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](numEltsLastBlock)))
        tmp = numBlocks - 1
        clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        tmp = numElements - numEltsLastBlock
        clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
        localSize = numThreadsLastBlock
        globalSize = numThreadsLastBlock
        global_work_size(0) = globalSize
        local_work_size(0) = localSize
        clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      }
    }
    else if (isPowerOfTwo(numElements)) {
      kernel = clCreateKernel(context.program, "prescan", null)
      clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
      if (same == 0) clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(inArray))
      else {
        val tmpP: cl_mem = clCreateBuffer(context.context, CL_MEM_READ_WRITE, Sizeof.cl_int, null, null)
        clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(tmpP))
      }
      val tmpP: cl_mem = clCreateBuffer(context.context, CL_MEM_READ_WRITE, Sizeof.cl_int, null, null)
      clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(tmpP))
      tmp = numThreads * 2
      clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 0
      clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 5, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 6, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 7, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 8, Sizeof.cl_int, Pointer.to(Array[Int](same)))
      clSetKernelArg(kernel, 9, sharedMemSize, null)
      localSize = numThreads
      globalSize = Math.max(1, numBlocks - np2LastBlock) * localSize
      val global_work_size = Array[Long](1)
      val local_work_size = Array[Long](1)
      global_work_size(0) = globalSize
      local_work_size(0) = localSize
      clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    }
    else {
      kernel = clCreateKernel(context.program, "prescan", null)
      clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(outArray))
      if (same == 0) clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(inArray))
      else {
        val tmpP: cl_mem = clCreateBuffer(context.context, CL_MEM_READ_WRITE, Sizeof.cl_int, null, null)
        clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(tmpP))
      }
      clSetKernelArg(kernel, 2, Sizeof.cl_mem, null)
      clSetKernelArg(kernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](numElements)))
      tmp = 0
      clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 5, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 6, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      tmp = 1
      clSetKernelArg(kernel, 7, Sizeof.cl_int, Pointer.to(Array[Int](tmp)))
      clSetKernelArg(kernel, 8, Sizeof.cl_int, Pointer.to(Array[Int](same)))
      clSetKernelArg(kernel, 9, sharedMemSize, null)
      localSize = numThreads
      globalSize = Math.max(1, numBlocks - np2LastBlock) * localSize
      val global_work_size = Array[Long](globalSize)
      val local_work_size = Array[Long](localSize)
      clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    }
    clFinish(context.queue)
  }

  def deallocBlockSums {
    g_scanBlockSums.foreach(clReleaseMemObject(_))
    g_numEltsAllocated = 0
    g_numLevelsAllocated = 0
  }

  def prescanArray(outArray: cl_mem, inArray: cl_mem, numElements: Int, context: OpenCLContext) {
    val prefixsumStartTime = System.nanoTime
    prescanArrayRecursive(outArray, inArray, numElements, 0, 0, context)
    val prefixsumEndTime = System.nanoTime

    println("Prefix Sum time = %,12d".format(prefixsumEndTime - prefixsumStartTime))
  }

  def scanImpl(d_input: cl_mem, rLen: Int, d_output: cl_mem, openCLContext: OpenCLContext) {
    preallocBlockSums(rLen, openCLContext)
    prescanArray(d_output, d_input, rLen, openCLContext)
    deallocBlockSums
  }

  def compute(col: Array[Int], value: Int, comp: Int, globalSize: Long, localSize: Long): Int = {
    val tupleNum: Long = col.length.toLong
    if (openCLContext == null) {
      openCLContext = new OpenCLContext
      openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
    }
    val start: Long = System.nanoTime
    gpuCol = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)

    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * tupleNum, Pointer.to(col), 0, null, null)

    gpuFilter = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = comp match {
      case 0 =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_eq", null)
      case 1 =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_gth", null)
      case 2 =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_geq", null)
      case 3 =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_lth", null)
      case 4 =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_leq", null)
      case _ =>
        clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    }
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    kernel = clCreateKernel(openCLContext.getOpenCLProgram, "countScanNum", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    val startPsum = System.nanoTime()
    scanImpl(gpuCount, globalSize.asInstanceOf[Int], gpuPsum, openCLContext)
    val endPsum = System.nanoTime()

    val tmp1 = Array[Int](0)
    val tmp2 = Array[Int](0)

    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuCount, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp1), 0, null, null)

    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuPsum, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp2), 0, null, null)

    resCount = tmp1(0) + tmp2(0)
    val end: Long = System.nanoTime
    resCount
  }

  def project(inCol: Array[Int], outCol: Array[Int]) {
    var kernel: cl_kernel = null
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    val tupleNum: Int = inCol.length
    val scanCol: cl_mem = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE,
      Sizeof.cl_int * tupleNum, null, null)
    println("Global size = %,12d".format(globalSize))
    println("in size = %,12d".format(tupleNum))
    println("out size = %,12d".format(outCol.length))

    println("Column data %s".format(inCol.mkString))
    hostToDeviceCopy(Pointer.to(inCol), scanCol, Sizeof.cl_int * inCol.length)
    val result: cl_mem = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE,
        Sizeof.cl_int * outCol.length, null, null)

    val psumVals = new Array[Int](globalSize)
    deviceToHostCopy(gpuPsum, Pointer.to(psumVals), globalSize * Sizeof.cl_int)
    println("psumVals data %s".format(psumVals.mkString))

    val filterVals = new Array[Int](tupleNum)
    deviceToHostCopy(gpuFilter, Pointer.to(filterVals), tupleNum * Sizeof.cl_int)
    println("filterVals %s".format(filterVals.mkString))

    kernel = clCreateKernel(openCLContext.getOpenCLProgram, "scan_int", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](Sizeof.cl_int)))
    clSetKernelArg(kernel, 2, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuPsum))
    clSetKernelArg(kernel, 4, Sizeof.cl_long, Pointer.to(Array[Long](resCount)))
    clSetKernelArg(kernel, 5, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 6, Sizeof.cl_mem, Pointer.to(result))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, result, CL_TRUE, 0, Sizeof.cl_int * resCount, Pointer.to(outCol), 0, null, null)
  }

  def releaseCol(col: cl_mem) {
    clReleaseMemObject(col)
  }

  def prefixSum(counts: Array[Int], prefixSums: Array[Int]): Unit = {

    if (counts.length != prefixSums.length) {
      throw new IllegalArgumentException("Input and output arrays should have the same size (%,12d != %,12d)".format(counts.length, prefixSums.length))
    }

    val globalSize = POW_2_S.filter(_ >= counts.length).head
    val localSize = Math.min(globalSize, BLOCK_SIZE)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)

    // using double buffers to avoid copying data
    val buffer1 = createReadWriteBuffer(Sizeof.cl_int * globalSize)
    val buffer2 = createReadWriteBuffer(Sizeof.cl_int * globalSize)
    hostToDeviceCopy(Pointer.to(counts), buffer1, Sizeof.cl_int * counts.length)
    val kernel = clCreateKernel(openCLContext.getOpenCLProgram, "prefix_sum_stage", null)
    var stride: Int = 0

    var switchedBuffers = true

    while (stride <= counts.length) {
      clSetKernelArg(kernel, if (switchedBuffers) 0 else 1, Sizeof.cl_mem, Pointer.to(buffer1))
      clSetKernelArg(kernel, if (switchedBuffers) 1 else 0, Sizeof.cl_mem, Pointer.to(buffer2))
      clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](stride)))
      clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      switchedBuffers = !switchedBuffers
      stride = if (stride == 0) 1 else stride << 1
    }
    val results = if (switchedBuffers) buffer1 else buffer2
    deviceToHostCopy(results, Pointer.to(prefixSums), Sizeof.cl_int * globalSize)
  }

  def scan(sourceCol: Array[Int], filter: Array[Int], prefixSums: Array[Int], destColumn: Array[Int], count: Int): Unit = {
    if (sourceCol.length != filter.length
      || filter.length != prefixSums.length) {
      throw new IllegalArgumentException("All arrays should have the same size(%,12d, %,12d, %,12d, %,12d)".
        format(sourceCol.length, filter.length, prefixSums.length, destColumn.length))
    }

    if (prefixSums.reverse.head != destColumn.length) {
      throw new IllegalArgumentException(("Size of dest coulumn is not the same as number of " +
        "filtered columns (%,12d, %,12d)").format(prefixSums.reverse.head, destColumn.length))
    }

    val globalSize = POW_2_S.filter(_ >= count).head
    val localSize = Math.min(globalSize, BLOCK_SIZE)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    val resultSize = destColumn.length


    val d_sourceColumns = createReadBuffer(Sizeof.cl_int * globalSize)
    val d_selectionFilter = createReadBuffer(Sizeof.cl_int * globalSize)
    val d_prefixSums = createReadBuffer(Sizeof.cl_int * globalSize)
    val d_destColumn = createReadWriteBuffer(Sizeof.cl_int * globalSize)

    hostToDeviceCopy(Pointer.to(sourceCol), d_sourceColumns, Sizeof.cl_int * count)
    hostToDeviceCopy(Pointer.to(filter), d_selectionFilter, Sizeof.cl_int * count)
    hostToDeviceCopy(Pointer.to(prefixSums), d_prefixSums, Sizeof.cl_int * count)

    val kernel = clCreateKernel(openCLContext.getOpenCLProgram, "scan", null)

    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(d_sourceColumns))
    clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(d_selectionFilter))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(d_prefixSums))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(d_destColumn))
    clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](count)))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    deviceToHostCopy(d_destColumn, Pointer.to(destColumn), Sizeof.cl_int * resultSize)
  }

  def selection(columnData: Array[Int], isBlocking: Boolean = true): Array[Int] = {

    val waitEvents = Array(new cl_event)

    val startInitTime = System.nanoTime
    val globalSize = POW_2_S.filter(_ >= columnData.length).head

    val localSize = Math.min(globalSize, 256)
    val global_work_size = Array[Long](globalSize)

    val local_work_size = Array[Long](localSize)
    if (openCLContext == null) {
      openCLContext = new OpenCLContext
      openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
    }


    val endInitTime = System.nanoTime

    val startTransferTime = System.nanoTime
    val columnBuffer = createReadBuffer(Sizeof.cl_int * globalSize)
    hostToDeviceCopy(Pointer.to(columnData), columnBuffer, Sizeof.cl_int * columnData.length)


    val endTransferTime = System.nanoTime

    val startFilterTime = System.nanoTime
    val filterBuffer = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)

    val filterKernel = clCreateKernel(openCLContext.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    clSetKernelArg(filterKernel, 0, Sizeof.cl_mem, Pointer.to(columnBuffer))
    clSetKernelArg(filterKernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](columnData.length.toLong)))
    clSetKernelArg(filterKernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(filterKernel, 3, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, filterKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)


    val endFilterTime = System.nanoTime
    val startPrefixSumTime = System.nanoTime
    // using double buffers to avoid copying data
    val prefixSumBuffer1 = createReadWriteBuffer(Sizeof.cl_int * globalSize)

    val prefixSumBuffer2 = createReadWriteBuffer(Sizeof.cl_int * globalSize)
    val copyKernel = clCreateKernel(openCLContext.getOpenCLProgram, "copy_buffer", null)
    clSetKernelArg(copyKernel, 0, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clSetKernelArg(copyKernel, 1, Sizeof.cl_mem, Pointer.to(prefixSumBuffer1))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, copyKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)

    val prefixSumKernel = clCreateKernel(openCLContext.getOpenCLProgram, "prefix_sum_stage", null)

    var stride: Int = 0

    var switchedBuffers = true
    while (stride <= columnData.length) {
      clSetKernelArg(prefixSumKernel, if (switchedBuffers) 0 else 1, Sizeof.cl_mem, Pointer.to(prefixSumBuffer1))
      clSetKernelArg(prefixSumKernel, if (switchedBuffers) 1 else 0, Sizeof.cl_mem, Pointer.to(prefixSumBuffer2))
      clSetKernelArg(prefixSumKernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](stride)))
      clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, prefixSumKernel, 1, null,
        global_work_size, local_work_size, 0, null, waitEvents(0))
      switchedBuffers = !switchedBuffers
      stride = if (stride == 0) 1 else stride << 1
      clWaitForEvents(1, waitEvents)
    }
    val prefixSumBuffer = if (switchedBuffers) prefixSumBuffer1 else prefixSumBuffer2

    val endPrefixSumTime = System.nanoTime

    val startFetchSizeTime = System.nanoTime
    val resultSize = Array(columnData.length)

    deviceToHostCopy(prefixSumBuffer, Pointer.to(resultSize), columnData.length, 0)
    val endFetchSizeTime = System.nanoTime

    val d_destColumn = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE,
      Sizeof.cl_int * resultSize.head, null, null)

    val startScanTime = System.nanoTime
    val scanKernel = clCreateKernel(openCLContext.getOpenCLProgram, "scan", null)

    clSetKernelArg(scanKernel, 0, Sizeof.cl_mem, Pointer.to(columnBuffer))
    clSetKernelArg(scanKernel, 1, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clSetKernelArg(scanKernel, 2, Sizeof.cl_mem, Pointer.to(prefixSumBuffer))
    clSetKernelArg(scanKernel, 3, Sizeof.cl_mem, Pointer.to(d_destColumn))
    clSetKernelArg(scanKernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](resultSize.head)))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, scanKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)

    val endScanTime = System.nanoTime

    val startCopyResultTime = System.nanoTime
    val destColumn = new Array[Int](resultSize.head)
    deviceToHostCopy(d_destColumn, Pointer.to(destColumn), Sizeof.cl_int * resultSize.head)

    val endCopyResultTime = System.nanoTime

    println("Times (%12s | %12s | %12s | %12s | %12s | %12s)".format(
      "Transfer2GPU", "Filter", "PrefixSum", "FetchSize", "LastScan", "Transfer2Host"
    ))
    println("Times (%,12d | %,12d | %,12d | %,12d | %,12d | %,12d)".format(
      -(startTransferTime - endTransferTime),
      -(startFilterTime - endFilterTime),
      -(startPrefixSumTime - endPrefixSumTime),
      -(startFetchSizeTime - endFetchSizeTime),
      -(startScanTime - endScanTime),
      -(startCopyResultTime - endCopyResultTime)
    ))
    destColumn
  }

  def nonBlockingSelection(columnData: Array[Int]) = selection(columnData, false)

  private def createReadBuffer(size: Long): cl_mem = {
    clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_ONLY, size, null, null)

  }

  private def createReadWriteBuffer(size: Long): cl_mem = {
    clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, size, null, null)
  }

  private def createWriteBuffer(size: Long): cl_mem = {
    clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_WRITE_ONLY, size, null, null)
  }

  private def hostToDeviceCopy(src: Pointer, dest: cl_mem, length: Long): Unit = {
    clEnqueueWriteBuffer(openCLContext.getOpenCLQueue, dest, CL_TRUE, 0, length, src,
      0, null, null)
  }

  private def deviceToHostCopy(src: cl_mem, dest: Pointer, length: Long, offset: Long = 0): Unit = {
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, src, CL_TRUE, offset, length, dest, 0, null,
      null)
  }

  def release {
    clReleaseMemObject(gpuPsum)
    clReleaseMemObject(gpuCount)
    clReleaseMemObject(gpuCol)
    clReleaseMemObject(gpuFilter)
  }

  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)
  val BLOCK_SIZE: Int = 256
  val NUM_BANKS: Int = 16
  val LOG_NUM_BANKS: Int = 4
  var g_scanBlockSums: Array[cl_mem] = null
  var g_numEltsAllocated: Int = 0
  var g_numLevelsAllocated: Int = 0

  var gpuCol: cl_mem = null
  var gpuFilter: cl_mem = null
  var gpuCount: cl_mem = null
  var gpuPsum: cl_mem = null

  override def hasNext: Boolean = itr.hasNext

  var globalSize = 0

  var localSize = 0
  var resCount = 0

  override def next(): RDDChunk[T] = {
    val chunk = new RDDChunk[T](columnTypes)
    val startTransformDataTime = System.nanoTime
    chunk.fill(itr)
    val endTransformDataTime = System.nanoTime
    val startSelectionTotalTime = System.nanoTime

    if (columnTypes(colIndex) == "INT") {
      val data = chunk.intData(colIndex).take(chunk.actualSize)
      localSize = math.min(256, chunk.intData(colIndex).length)
      globalSize = localSize * math.min(1 + (chunk.intData(colIndex).length - 1) / localSize, 2048)

      val resultSize = compute(data, value, operation, globalSize, localSize)

      println("actualSize value = %,12d".format(chunk.actualSize))
      println("result value = %,12d".format(resultSize))
      val outData = new Array[Int](resultSize)
      chunk.actualSize = resultSize
      project(data, outData)

      println("Out Data: ")
      println(outData.mkString(", "))
      chunk.actualSize = resCount
      chunk.intData(colIndex) = outData
    }
    val endSelectionTotalTime = System.nanoTime

    val totalTime = endSelectionTotalTime - startTransformDataTime
    println("Test with size=%,12d".format(chunk.actualSize))
    println("Total transform time (ns) to copy %,12d elements of data = %,12d".format
      (-1, endTransformDataTime - startTransformDataTime))
    println("Selection time (ns) = %,12d".format
      (endSelectionTotalTime - startSelectionTotalTime))
    println("Total selection time (ns) = %,12d".format
      (totalTime))

    dept += 1

    if (dept >= 2) {
      throw new RuntimeException("Too many times calling into this function")
    }


    chunk
  }


  var dept = 0


}


