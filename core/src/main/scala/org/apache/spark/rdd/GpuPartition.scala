package org.apache.spark.rdd

import org.apache.spark.Logging
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}

class GpuPartition[T <: Product : ClassTag](val columnTypes: Array[String], val capacity: Int)
  extends Serializable with Logging {

  def MAX_STRING_SIZE: Int = 1 << 7

  var context: OpenCLContext = null
  var size = 0

  val intData: Array[Array[Int]] = Array.ofDim[Int](columnTypes.filter(_ == "INT").length, capacity)
  val longData = Array.ofDim[Long](columnTypes.filter(_ == "LONG").length, capacity)
  val floatData = Array.ofDim[Float](columnTypes.filter(_ == "FLOAT").length, capacity)
  val doubleData = Array.ofDim[Double](columnTypes.filter(_ == "DOUBLE").length, capacity)
  val booleanData = Array.ofDim[Boolean](columnTypes.filter(_ == "BOOLEAN").length, capacity)
  val charData = Array.ofDim[Char](columnTypes.filter(_ == "CHAR").length, capacity)
  val stringData = Array.ofDim[Char](columnTypes.filter(_ == "STRING").length
    , capacity * MAX_STRING_SIZE)

  def inferBestWorkGroupSize(): Unit = {
    this.localSize = math.min(BLOCK_SIZE, size)
    this.globalSize = localSize * math.min(1 + (size - 1) / localSize, BLOCK_SIZE)
  }


  def fill(iter: Iterator[T]): Unit = {
    size = 0
    val values: Iterator[T] = iter.take(capacity)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        size = rowIndex + 1
        v.productIterator.zipWithIndex.foreach { case (p, colIndex) =>
          if (columnTypes(colIndex) == "INT") {
            intData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Int]
          } else if (columnTypes(colIndex) == "LONG") {
            longData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Long]
          } else if (columnTypes(colIndex) == "FLOAT") {
            floatData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Float]
          } else if (columnTypes(colIndex) == "Double") {
            doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Double]
          } else if (columnTypes(colIndex) == "BOOLEAN") {
            booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Boolean]
          } else if (columnTypes(colIndex) == "CHAR") {
            charData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Char]
          } else if (columnTypes(colIndex) == "STRING") {
            val str = p.toString
            str.getChars(0, Math.min(MAX_STRING_SIZE, str.length),
              stringData(toTypeAwareColumnIndex(colIndex)), rowIndex * MAX_STRING_SIZE)
          }
        }
    }
  }

  def getStringData(typeAwareColumnIndex: Int, rowIndex: Int): String = {
    val str = new String(stringData(typeAwareColumnIndex), rowIndex * MAX_STRING_SIZE, MAX_STRING_SIZE)
    val actualLenght = str.indexOf(0)
    str.substring(0, actualLenght)
  }

  /**
   * Returns how many columns with the same type appear before the given column
   * in the underlying type of this chunk. For example, if the underlying type of the chunk is
   * (Int, Int, String, Int, String) the return values will be (0, 1, 0, 2, 1).
   *
   * @param columnIndex The given column index
   * @return Number of columns with the same type as the given column
   */
  def toTypeAwareColumnIndex(columnIndex: Int): Int = {
    val targetColumnType = columnTypes(columnIndex)
    val (taken, _) = columnTypes.splitAt(columnIndex)
    taken.filter(_ == targetColumnType).length
  }

  def apply(rowIndex: Int): T = {

    val values: Array[Any] = columnTypes.zipWithIndex.map({ case (colType, colIndex) =>
      if (colType == "INT") {
        intData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "LONG") {
        longData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "FLOAT") {
        floatData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "Double") {
        doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "BOOLEAN") {
        booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "CHAR") {
        charData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (columnTypes(colIndex) == "STRING") {
        getStringData(toTypeAwareColumnIndex(colIndex), rowIndex)
      }
    })

    val resultTuple = values.length match {
      case 2 => (values(0), values(1)).asInstanceOf[T]
      case 3 => (values(0), values(1), values(2)).asInstanceOf[T]
      case 4 => (values(0), values(1), values(2), values(3)).asInstanceOf[T]
      case 5 => (values(0), values(1), values(2), values(3), values(4)).asInstanceOf[T]
      case 6 => (values(0), values(1), values(2), values(3), values(4), values(5)).asInstanceOf[T]
      case 7 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6)).asInstanceOf[T]
      case 8 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7)).asInstanceOf[T]
      case 9 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8)).asInstanceOf[T]
      case 10 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9)).asInstanceOf[T]
      case 11 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10)).asInstanceOf[T]
      case 12 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11)).asInstanceOf[T]
      case 13 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12)).asInstanceOf[T]
      case 14 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13)).asInstanceOf[T]
      case 15 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14)).asInstanceOf[T]
      case 16 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15)).asInstanceOf[T]
      case 17 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16)).asInstanceOf[T]
      case 18 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17)).asInstanceOf[T]
      case 19 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18)).asInstanceOf[T]
      case 20 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19)).asInstanceOf[T]
      case 21 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19), values(20)).asInstanceOf[T]
      case 22 => (values(0), values(1), values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9), values(10), values(11), values(12), values(13), values(14), values(15), values(16), values(17), values(18), values(19), values(20), values(21)).asInstanceOf[T]
      case _ => throw new NotImplementedError("org.apache.spark.rdd.GpuPartition.apply is not " +
        "implemented yet")

    }
    resultTuple
  }

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

  def preallocBlockSums(maxNumElements: Int) {
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

  def prescanArrayRecursive(outArray: cl_mem, inArray: cl_mem, numElements: Int, level: Int,
                            same: Int) {

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
      prescanArrayRecursive(g_scanBlockSums(level), g_scanBlockSums(level), numBlocks, level + 1, 1)
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
    } else if (isPowerOfTwo(numElements)) {
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
    } else {
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

  def prescanArray(outArray: cl_mem, inArray: cl_mem, numElements: Int) {
    val prefixsumStartTime = System.nanoTime
    prescanArrayRecursive(outArray, inArray, numElements, 0, 0)
    val prefixsumEndTime = System.nanoTime

    println("Prefix Sum time = %,12d".format(prefixsumEndTime - prefixsumStartTime))
  }

  def scanImpl(d_input: cl_mem, rLen: Int, d_output: cl_mem) {
    preallocBlockSums(rLen)
    prescanArray(d_output, d_input, rLen)
    deallocBlockSums
  }

  def pointer[T: ClassTag : TypeTag](values: Array[T]): Pointer = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSym = mirror.classSymbol(values.getClass.getComponentType)
    if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Int]].tpe) {
      Pointer.to(values.asInstanceOf[Array[Int]])
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Long]].tpe) {
      Pointer.to(values.asInstanceOf[Array[Long]])
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Float]].tpe) {
      Pointer.to(values.asInstanceOf[Array[Float]])
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Double]].tpe) {
      Pointer.to(values.asInstanceOf[Array[Double]])
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Char]].tpe) {
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Char]].tpe) { //TODO fix Strings
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else {
      throw new NotImplementedError("Cannot create a pointer to an array of %s.".format(
        implicitly[TypeTag[T]].tpe.toString))
    }
  }

  def compute[T: ClassTag : TypeTag](col: Array[T], tupleNum: Long, value: T, comp: Int, globalSize: Long,
                                     localSize: Long): Int = {
    if (context == null) {
      context = new OpenCLContext
      context.initOpenCL("/org/apache/spark/gpu/kernel.cl")
    }
    val start: Long = System.nanoTime
    gpuCol = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)

    clEnqueueWriteBuffer(context.getOpenCLQueue, gpuCol, CL_TRUE, 0, Sizeof.cl_int * tupleNum,
      pointer(col), 0, null, null)

    gpuFilter = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * tupleNum, null, null)
    gpuPsum = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    gpuCount = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)
    var kernel = comp match {
      case 0 =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_eq", null)
      case 1 =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_gth", null)
      case 2 =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_geq", null)
      case 3 =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_lth", null)
      case 4 =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_leq", null)
      case _ =>
        clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    }
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_int, pointer(Array(value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    kernel = clCreateKernel(context.getOpenCLProgram, "countScanNum", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    val startPsum = System.nanoTime()
    scanImpl(gpuCount, globalSize.asInstanceOf[Int], gpuPsum)
    val endPsum = System.nanoTime()

    val tmp1 = Array[Int](0)
    val tmp2 = Array[Int](0)

    clEnqueueReadBuffer(context.getOpenCLQueue, gpuCount, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp1), 0, null, null)

    clEnqueueReadBuffer(context.getOpenCLQueue, gpuPsum, CL_TRUE, Sizeof.cl_int * (globalSize - 1), Sizeof.cl_int, Pointer.to(tmp2), 0, null, null)

    resCount = tmp1(0) + tmp2(0)
    val end: Long = System.nanoTime
    resCount
  }

  def project(inCol: Array[Int], tupleNum: Int, outCol: Array[Int], outSize: Int) {
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    val scanCol: cl_mem = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE,
      Sizeof.cl_int * tupleNum, null, null)

    hostToDeviceCopy[Int](Pointer.to(inCol), scanCol, tupleNum)
    val result: cl_mem = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE,
      Sizeof.cl_int * outSize, null, null)

    val psumVals = new Array[Int](globalSize)
    deviceToHostCopy[Int](gpuPsum, Pointer.to(psumVals), globalSize)

    val filterVals = new Array[Int](tupleNum)
    deviceToHostCopy[Int](gpuFilter, Pointer.to(filterVals), tupleNum)

    val kernel = clCreateKernel(context.getOpenCLProgram, "scan_int", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](Sizeof.cl_int)))
    clSetKernelArg(kernel, 2, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuPsum))
    clSetKernelArg(kernel, 4, Sizeof.cl_long, Pointer.to(Array[Long](resCount)))
    clSetKernelArg(kernel, 5, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 6, Sizeof.cl_mem, Pointer.to(result))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    clEnqueueReadBuffer(context.getOpenCLQueue, result, CL_TRUE, 0, Sizeof.cl_int * outSize, Pointer.to(outCol), 0, null, null)
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
    val buffer1 = createReadWriteBuffer[Int](globalSize.toInt)
    val buffer2 = createReadWriteBuffer[Int](globalSize.toInt)
    hostToDeviceCopy[Int](Pointer.to(counts), buffer1, counts.length)
    val kernel = clCreateKernel(context.getOpenCLProgram, "prefix_sum_stage", null)
    var stride: Int = 0

    var switchedBuffers = true

    while (stride <= counts.length) {
      clSetKernelArg(kernel, if (switchedBuffers) 0 else 1, Sizeof.cl_mem, Pointer.to(buffer1))
      clSetKernelArg(kernel, if (switchedBuffers) 1 else 0, Sizeof.cl_mem, Pointer.to(buffer2))
      clSetKernelArg(kernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](stride)))
      clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
      switchedBuffers = !switchedBuffers
      stride = if (stride == 0) 1 else stride << 1
    }
    val results = if (switchedBuffers) buffer1 else buffer2
    deviceToHostCopy[Int](results, Pointer.to(prefixSums), globalSize)
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

    val d_sourceColumns = createReadBuffer[Int](globalSize.toInt)
    val d_selectionFilter = createReadBuffer[Int](globalSize.toInt)
    val d_prefixSums = createReadBuffer[Int](globalSize.toInt)
    val d_destColumn = createReadWriteBuffer[Int](globalSize.toInt)

    hostToDeviceCopy[Int](Pointer.to(sourceCol), d_sourceColumns, count)
    hostToDeviceCopy[Int](Pointer.to(filter), d_selectionFilter, count)
    hostToDeviceCopy[Int](Pointer.to(prefixSums), d_prefixSums, count)

    val kernel = clCreateKernel(context.getOpenCLProgram, "scan", null)

    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(d_sourceColumns))
    clSetKernelArg(kernel, 1, Sizeof.cl_mem, Pointer.to(d_selectionFilter))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(d_prefixSums))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(d_destColumn))
    clSetKernelArg(kernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](count)))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    deviceToHostCopy[Int](d_destColumn, Pointer.to(destColumn), resultSize)
  }

  def getColumn[V: ClassTag](columnIndex: Int): Array[V] = {
    val typeAwareColumnIndex = toTypeAwareColumnIndex(columnIndex)

    implicitly[ClassTag[V]] match {
      case ClassTag.Int => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case ClassTag.Long => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case ClassTag.Float => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case ClassTag.Double => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case ClassTag.Char => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      // TODO fix  the String type
      // case implicitly[ClassTag[String]] => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case _ => throw new NotImplementedError("Unknown type ")
    }
  }

  def baseSize[V: ClassTag](): Int = {
    implicitly[ClassTag[V]] match {
      case ClassTag.Int => Sizeof.cl_int
      case ClassTag.Long => Sizeof.cl_long
      case ClassTag.Float => Sizeof.cl_float
      case ClassTag.Double => Sizeof.cl_double
      case ClassTag.Char => Sizeof.cl_char
      // TODO fix  the String type
      case _ => throw new NotImplementedError("Unknown type %s".format(implicitly[ClassTag[V]]))
    }
  }

  def typeNameString[V: ClassTag](): String = {
    implicitly[ClassTag[V]] match {
      case ClassTag.Int => "int"
      case ClassTag.Long => "long"
      case ClassTag.Float => "float"
      case ClassTag.Double => "double"
      case ClassTag.Char => "char"
      // TODO fix  the String type
      case _ => throw new NotImplementedError("Unknown type ")
    }
  }

  def filter[V: ClassTag : TypeTag](columnIndex: Int, value: V, operation: ComparisonOperation.Value):
  Int = {

    val start: Long = System.nanoTime
    val tupleNum = this.size
    gpuCol = createReadWriteBuffer[V](tupleNum)

    val col = getColumn[V](columnIndex)
    hostToDeviceCopy[V](pointer(col), gpuCol, tupleNum)

    gpuFilter = createReadWriteBuffer[Int](tupleNum)
    gpuPsum = createReadWriteBuffer[Int](globalSize)
    gpuCount = createReadWriteBuffer[Int](globalSize)

    val operationName = operation.toString

    val typeName = typeNameString[V]()

    val kernelName = "genScanFilter_init_%s_%s".format(typeName, operationName)

    var kernel = clCreateKernel(context.getOpenCLProgram, kernelName, null)

    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, baseSize[V], pointer(Array(value)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    kernel = clCreateKernel(context.getOpenCLProgram, "countScanNum", null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    val startPsum = System.nanoTime()
    scanImpl(gpuCount, globalSize, gpuPsum)
    val endPsum = System.nanoTime()

    val tmp1 = Array[Int](0)
    val tmp2 = Array[Int](0)

    deviceToHostCopy[Int](gpuCount, pointer(tmp1), 1)

    deviceToHostCopy[Int](gpuPsum, pointer(tmp2), 1)

    resCount = tmp1(0) + tmp2(0)
    val end: Long = System.nanoTime
    resCount
  }

  def selection(columnData: Array[Int], value: Int, isBlocking: Boolean = true): Array[Int] = {

    val waitEvents = Array(new cl_event)

    val startInitTime = System.nanoTime
    val globalSize = POW_2_S.filter(_ >= columnData.length).head

    val localSize = Math.min(globalSize, 256)
    val global_work_size = Array[Long](globalSize)

    val local_work_size = Array[Long](localSize)
    if (context == null) {
      context = new OpenCLContext
      context.initOpenCL("/org/apache/spark/gpu/kernel.cl")
    }

    val endInitTime = System.nanoTime

    val startTransferTime = System.nanoTime
    val columnBuffer = createReadBuffer[Int](globalSize.toInt)
    hostToDeviceCopy[Int](Pointer.to(columnData), columnBuffer, columnData.length)

    val endTransferTime = System.nanoTime

    val startFilterTime = System.nanoTime
    val filterBuffer = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, Sizeof.cl_int * globalSize, null, null)

    val filterKernel = clCreateKernel(context.getOpenCLProgram, "genScanFilter_init_int_eq", null)
    clSetKernelArg(filterKernel, 0, Sizeof.cl_mem, Pointer.to(columnBuffer))
    clSetKernelArg(filterKernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](columnData.length.toLong)))
    clSetKernelArg(filterKernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](value)))
    clSetKernelArg(filterKernel, 3, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, filterKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)

    val endFilterTime = System.nanoTime
    val startPrefixSumTime = System.nanoTime
    // using double buffers to avoid copying data
    val prefixSumBuffer1 = createReadWriteBuffer[Int](globalSize.toInt)

    val prefixSumBuffer2 = createReadWriteBuffer[Int](globalSize.toInt)
    val copyKernel = clCreateKernel(context.getOpenCLProgram, "copy_buffer", null)
    clSetKernelArg(copyKernel, 0, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clSetKernelArg(copyKernel, 1, Sizeof.cl_mem, Pointer.to(prefixSumBuffer1))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, copyKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)

    val prefixSumKernel = clCreateKernel(context.getOpenCLProgram, "prefix_sum_stage", null)

    var stride: Int = 0

    var switchedBuffers = true
    while (stride <= columnData.length) {
      clSetKernelArg(prefixSumKernel, if (switchedBuffers) 0 else 1, Sizeof.cl_mem, Pointer.to(prefixSumBuffer1))
      clSetKernelArg(prefixSumKernel, if (switchedBuffers) 1 else 0, Sizeof.cl_mem, Pointer.to(prefixSumBuffer2))
      clSetKernelArg(prefixSumKernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](stride)))
      clEnqueueNDRangeKernel(context.getOpenCLQueue, prefixSumKernel, 1, null,
        global_work_size, local_work_size, 0, null, waitEvents(0))
      switchedBuffers = !switchedBuffers
      stride = if (stride == 0) 1 else stride << 1
      clWaitForEvents(1, waitEvents)
    }
    val prefixSumBuffer = if (switchedBuffers) prefixSumBuffer1 else prefixSumBuffer2

    val endPrefixSumTime = System.nanoTime

    val startFetchSizeTime = System.nanoTime
    val resultSize = Array(columnData.length)

    deviceToHostCopy[Int](prefixSumBuffer, Pointer.to(resultSize), columnData.length, 0)
    val endFetchSizeTime = System.nanoTime

    val d_destColumn = clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE,
      Sizeof.cl_int * resultSize.head, null, null)

    val startScanTime = System.nanoTime
    val scanKernel = clCreateKernel(context.getOpenCLProgram, "scan", null)

    clSetKernelArg(scanKernel, 0, Sizeof.cl_mem, Pointer.to(columnBuffer))
    clSetKernelArg(scanKernel, 1, Sizeof.cl_mem, Pointer.to(filterBuffer))
    clSetKernelArg(scanKernel, 2, Sizeof.cl_mem, Pointer.to(prefixSumBuffer))
    clSetKernelArg(scanKernel, 3, Sizeof.cl_mem, Pointer.to(d_destColumn))
    clSetKernelArg(scanKernel, 4, Sizeof.cl_int, Pointer.to(Array[Int](resultSize.head)))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, scanKernel, 1, null, global_work_size,
      local_work_size, 0, null, waitEvents(0))
    clWaitForEvents(1, waitEvents)

    val endScanTime = System.nanoTime

    val startCopyResultTime = System.nanoTime
    val destColumn = new Array[Int](resultSize.head)
    deviceToHostCopy[Int](d_destColumn, Pointer.to(destColumn), resultSize.head)

    val endCopyResultTime = System.nanoTime

    println("Times (%12s | %12s | %12s | %12s | %12s | %12s)".format(
      "Transfer2GPU", "Filter", "PrefixSum", "FetchSize", "LastScan", "Transfer2Host"))
    println("Times (%,12d | %,12d | %,12d | %,12d | %,12d | %,12d)".format(
      -(startTransferTime - endTransferTime),
      -(startFilterTime - endFilterTime),
      -(startPrefixSumTime - endPrefixSumTime),
      -(startFetchSizeTime - endFetchSizeTime),
      -(startScanTime - endScanTime),
      -(startCopyResultTime - endCopyResultTime)))
    destColumn
  }

  def nonBlockingSelection(columnData: Array[Int], value: Int) = selection(columnData, value, false)

  private def createReadBuffer[V: ClassTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_ONLY, size, null, null)
  }

  private def createReadWriteBuffer[V: ClassTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, size, null, null)
  }

  private def createWriteBuffer(size: Long): cl_mem = {
    clCreateBuffer(context.getOpenCLContext, CL_MEM_WRITE_ONLY, size, null, null)
  }

  private def hostToDeviceCopy[V: ClassTag](src: Pointer, dest: cl_mem, elementCount: Long): Unit = {
    val length = elementCount * baseSize[V]
    clEnqueueWriteBuffer(context.getOpenCLQueue, dest, CL_TRUE, 0, length, src,
      0, null, null)
  }

  private def deviceToHostCopy[V: ClassTag](src: cl_mem, dest: Pointer, elementCount: Long, offset: Long = 0): Unit = {
    val length = elementCount * baseSize[V]
    clEnqueueReadBuffer(context.getOpenCLQueue, src, CL_TRUE, offset, length, dest, 0, null,
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

  var globalSize = 0

  var localSize = 0

  var resCount = 0

}

class ComparisonOperation extends Enumeration {

}

object ComparisonOperation extends Enumeration {
  type WeekDay = Value
  val < = Value("lth")
  val <= = Value("leq")
  val > = Value("gth")
  val >= = Value("geq")
  val == = Value("eq")
  val != = Value("neq")
}
