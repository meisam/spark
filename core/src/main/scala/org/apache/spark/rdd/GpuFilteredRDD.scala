package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.apache.spark.{TaskContext, Partition}
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof, cl_kernel, cl_mem}

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
      var global_work_size = Array[Long](globalSize)
      var local_work_size = Array[Long](localSize)
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
        globalSize = ({
          localSize = numThreadsLastBlock;
          localSize
        })
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
      global_work_size = Array[Long](globalSize)
      local_work_size = Array[Long](localSize)
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
        globalSize = ({
          localSize = numThreadsLastBlock;
          localSize
        })
        global_work_size = Array[Long](globalSize)
        local_work_size = Array[Long](localSize)
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
      val global_work_size = Array[Long](globalSize)
      val local_work_size = Array[Long](localSize)
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
  }

  def deallocBlockSums {
    g_scanBlockSums.foreach(clReleaseMemObject(_))
    g_numEltsAllocated = 0
    g_numLevelsAllocated = 0
  }

  def prescanArray(outArray: cl_mem, inArray: cl_mem, numElements: Int, context: OpenCLContext) {
    prescanArrayRecursive(outArray, inArray, numElements, 0, 0, context)
  }

  def scanImpl(d_input: cl_mem, rLen: Int, d_output: cl_mem, openCLContext: OpenCLContext) {
    preallocBlockSums(rLen, openCLContext)
    prescanArray(d_output, d_input, rLen, openCLContext)
    deallocBlockSums
  }

  def compute(col: Array[Int], value: Int, comp: Int, globalSize: Long, localSize: Long) {
    val tupleNum: Long = col.length
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
    val global_work_size = Array[Long](1)
    global_work_size(0) = globalSize
    val local_work_size = Array[Long](1)
    local_work_size(0) = localSize
    println("global=%d, local= %d".format(globalSize, localSize))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    kernel = clCreateKernel(openCLContext.getOpenCLProgram, "countScanNum", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
    clEnqueueNDRangeKernel(openCLContext.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    gpuPsum = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    gpuCount = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    scanImpl(gpuCount, globalSize.asInstanceOf[Int], gpuPsum, openCLContext)
    val tmp1 = new Array[Int](1)
    val tmp2 = new Array[Int](1)
    println("Before tmp1 = %d, tmp2 = %d".format(tmp1(0), tmp2(0)))
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuCount, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp1), 0, null, null)
    clEnqueueReadBuffer(openCLContext.getOpenCLQueue, gpuPsum, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp2), 0, null, null)

    if (tmp1(0) < 0 || tmp2(0) < 0) {
      println("After tmp1 = %d, tmp2 = %d".format(tmp1(0), tmp2(0)))
      println("Integer overflow")
    }
    resCount = tmp1(0) + tmp2(0)
    val end: Long = System.nanoTime
  }

  def project(inCol: Array[Int], outCol: Array[Int]) {
    var kernel: cl_kernel = null
    val global_work_size = Array[Long](1)
    global_work_size(0) = globalSize
    val local_work_size = Array[Long](1)
    local_work_size(0) = localSize
    val tupleNum: Int = inCol.length
    println("tuple number = %d".format(tupleNum))
    val scanCol: cl_mem = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    //TOOD the result buffer does not need to be as big as the input buffer
    val result: cl_mem = clCreateBuffer(openCLContext.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    kernel = clCreateKernel(openCLContext.getOpenCLProgram, "scan_int", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](4)))
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

  def release {
    clReleaseMemObject(gpuPsum)
    clReleaseMemObject(gpuCount)
    clReleaseMemObject(gpuCol)
    clReleaseMemObject(gpuFilter)
  }

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
    chunk.fill(itr)
    if (columnTypes(colIndex) == "INT") {
      val data = chunk.intData(colIndex).take(chunk.actualSize)
      globalSize = data.length
      localSize = Math.min(BLOCK_SIZE, globalSize)
      compute(data, value, operation, globalSize, localSize)

      val outData = new Array[Int](chunk.actualSize)
      resCount = chunk.actualSize
      project(data, outData)
      println(outData.mkString(","))
      chunk.intData(colIndex) = outData
    }
    chunk
  }
}


