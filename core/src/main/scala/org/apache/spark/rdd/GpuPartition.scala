package org.apache.spark.rdd

import org.apache.spark.Logging
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.ClassTag
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

class GpuPartition[T <: Product : TypeTag]
(context: OpenCLContext, val capacity: Int)
  extends Serializable with Logging {

  type JavaType = JavaUniverse#Type

  val columnTypes = typeOf[T] match {
    case ru.TypeRef(tpe, sym, typeArgs) => typeArgs
    case _ => throw new NotImplementedError("Unknown type %s".format(typeOf[T]))
  }

  def MAX_STRING_SIZE: Int = 1 << 7
  def HASH_SIZE = 131072

  var size = 0

  val byteData = Array.ofDim[Byte](columnTypes.count(_ == TypeTag.Byte.tpe), capacity)
  val shortData = Array.ofDim[Short](columnTypes.count(_ == TypeTag.Short.tpe), capacity)
  val intData = Array.ofDim[Int](columnTypes.count(_ == TypeTag.Int.tpe), capacity)
  val longData = Array.ofDim[Long](columnTypes.count(_ == TypeTag.Long.tpe), capacity)
  val floatData = Array.ofDim[Float](columnTypes.count(_ == TypeTag.Float.tpe), capacity)
  val doubleData = Array.ofDim[Double](columnTypes.count(_ == TypeTag.Double.tpe), capacity)
  val booleanData = Array.ofDim[Boolean](columnTypes.count(_ == TypeTag.Boolean.tpe), capacity)
  val charData = Array.ofDim[Char](columnTypes.count(_ == TypeTag.Char.tpe), capacity)
  val stringData = Array.ofDim[Char](columnTypes.count(_ == ColumnarTypes.StringTypeTag.tpe)
    , capacity * MAX_STRING_SIZE)

  def inferBestWorkGroupSize(): Unit = {
    this.localSize = if (size == 0) 1 else math.min(BLOCK_SIZE, size)
    this.globalSize = localSize * math.min(1 + (size - 1) / localSize, BLOCK_SIZE)
  }


  def fill(iter: Iterator[T]): Unit = {
    size = 0
    val values: Iterator[T] = iter.take(capacity)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        size = rowIndex + 1
        v.productIterator.zipWithIndex.foreach { case (p, colIndex) =>
          if (columnTypes(colIndex) == TypeTag.Byte.tpe) {
            byteData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Byte]
          } else if (columnTypes(colIndex) == TypeTag.Short.tpe) {
            shortData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Short]
          } else if (columnTypes(colIndex) == TypeTag.Int.tpe) {
            intData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Int]
          } else if (columnTypes(colIndex) == TypeTag.Long.tpe) {
            longData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Long]
          } else if (columnTypes(colIndex) == TypeTag.Float.tpe) {
            floatData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Float]
          } else if (columnTypes(colIndex) == TypeTag.Double.tpe) {
            doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Double]
          } else if (columnTypes(colIndex) == TypeTag.Boolean.tpe) {
            booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Boolean]
          } else if (columnTypes(colIndex) == TypeTag.Char.tpe) {
            charData(toTypeAwareColumnIndex(colIndex))(rowIndex) = p.asInstanceOf[Char]
          } else if (columnTypes(colIndex) == ColumnarTypes.StringTypeTag) {
            val str = p.toString
            str.getChars(0, Math.min(MAX_STRING_SIZE, str.length),
              stringData(toTypeAwareColumnIndex(colIndex)), rowIndex * MAX_STRING_SIZE)
          }
        }
    }
    inferBestWorkGroupSize
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

    val values = columnTypes.zipWithIndex.map({ case (colType, colIndex) =>
      if (colType == TypeTag.Byte.tpe) {
        byteData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Int.tpe) {
        intData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Long.tpe) {
        longData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Float.tpe) {
        floatData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Double.tpe) {
        doubleData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Boolean.tpe) {
        booleanData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == TypeTag.Char.tpe) {
        charData(toTypeAwareColumnIndex(colIndex))(rowIndex)
      } else if (colType == ColumnarTypes.StringTypeTag) {
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
    prescanArrayRecursive(outArray, inArray, numElements, 0, 0)
  }

  def scanImpl(d_input: cl_mem, rLen: Int, d_output: cl_mem) {
    preallocBlockSums(rLen)
    prescanArray(d_output, d_input, rLen)
    deallocBlockSums
  }

  def toArray[X:TypeTag](value: X): Array[X] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    implicit val xClassTag = ClassTag[X]( mirror.runtimeClass(typeOf[X]) )
    Array[X](value)
  }

  def pointer[T: TypeTag](values: Array[T]): Pointer = {

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
    } else if (implicitly[TypeTag[T]].tpe =:= implicitly[TypeTag[Char]].tpe) {
      //TODO fix Strings
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else {
      throw new NotImplementedError("Cannot create a pointer to an array of %s.".format(
        implicitly[TypeTag[T]].tpe.toString))
    }
  }

  def project[V: ClassTag : TypeTag](columnIndex: Int, outSize: Int) {
    if (outSize == 0)
      return
    val colData: Array[V] = getColumn(columnIndex)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    val scanCol: cl_mem = createReadWriteBuffer[V](size)

    hostToDeviceCopy[V](pointer(colData), scanCol, size)

    val result: cl_mem = createReadWriteBuffer[V](outSize)

    val columnTypeName = typeNameString[V]
    val kernelName: String = "scan_%s".format(columnTypeName)
    val kernel = clCreateKernel(context.getOpenCLProgram, kernelName, null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](Sizeof.cl_int)))
    clSetKernelArg(kernel, 2, Sizeof.cl_long, Pointer.to(Array[Long](size)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuPsum))
    clSetKernelArg(kernel, 4, Sizeof.cl_long, Pointer.to(Array[Long](resCount)))
    clSetKernelArg(kernel, 5, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 6, Sizeof.cl_mem, Pointer.to(result))
    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)
    deviceToHostCopy[V](result, pointer(colData), outSize)
  }

  def releaseCol(col: cl_mem) {
    clReleaseMemObject(col)
  }

  def getColumn[V: TypeTag](columnIndex: Int): Array[V] = {
    val typeAwareColumnIndex = toTypeAwareColumnIndex(columnIndex)

    implicitly[TypeTag[V]] match {
      case TypeTag.Byte => byteData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Short => shortData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Char => charData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Int => intData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Long => longData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Float => floatData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Double => doubleData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case TypeTag.Boolean => booleanData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case ColumnarTypes.StringTypeTag => stringData(typeAwareColumnIndex).asInstanceOf[Array[V]]
      case _ => throw new NotImplementedError("Unknown type %s".format(implicitly[TypeTag[V]]))
    }
  }

    }
  }

  def baseSize[V: ClassTag](): Int = {
    baseSize2(implicitly[ClassTag[V]])
  }

  def baseSize2(ct: ClassTag[_]): Int = {
    ct match {
      case ClassTag.Byte => Sizeof.cl_char
      case ClassTag.Short => Sizeof.cl_short
      case ClassTag.Int => Sizeof.cl_int
      case ClassTag.Long => Sizeof.cl_long
      case ClassTag.Float => Sizeof.cl_float
      case ClassTag.Double => Sizeof.cl_double
      case ClassTag.Boolean => Sizeof.cl_char // NOTE C and Java primitive types have different sizes
      case ClassTag.Char => Sizeof.cl_short // NOTE C and Java primitive types have different sizes
      // TODO fix  the String type
      case _ => throw new NotImplementedError("Unknown type %s".format(ct.toString()))
    }
  }

  def typeNameString[V: ClassTag](): String = {
    implicitly[ClassTag[V]] match {
      case ClassTag.Int => "int"
      case ClassTag.Long => "long"
      case ClassTag.Float => "float"
      case ClassTag.Double => "double"
      case ClassTag.Boolean => "boolean"
      case ClassTag.Char => "char"
      // TODO fix  the String type
      case _ => throw new NotImplementedError("Unknown type %s".format(implicitly[ClassTag[V]]))
    }
  }

  def dataPosition(columnIndex: Int) = {
    //TODO For now, data is always stored on device (cpu)
    DataPosition.HOST
  }

  def filter[V: ClassTag : TypeTag](columnIndex: Int, value: V, operation: ComparisonOperation.Value):
  Int = {

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

    scanImpl(gpuCount, globalSize, gpuPsum)

    val tmp1 = Array[Int](0)
    val tmp2 = Array[Int](0)

    deviceToHostCopy[Int](gpuCount, pointer(tmp1), 1, globalSize - 1)

    deviceToHostCopy[Int](gpuPsum, pointer(tmp2), 1, globalSize - 1)

    resCount = tmp1(0) + tmp2(0)
    resCount
  }

  protected def createReadBuffer[V: ClassTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_ONLY, size, null, null)
  }

  protected def createReadWriteBuffer[V: ClassTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.getOpenCLContext, CL_MEM_READ_WRITE, size, null, null)
  }

  protected def createWriteBuffer(size: Long): cl_mem = {
    clCreateBuffer(context.getOpenCLContext, CL_MEM_WRITE_ONLY, size, null, null)
  }

  protected def hostToDeviceCopy[V: ClassTag](src: Pointer, dest: cl_mem, elementCount: Long,
                                              offset: Int = 0): Unit = {
    val length = elementCount * baseSize[V]
    clEnqueueWriteBuffer(context.getOpenCLQueue, dest, CL_TRUE, offset, length, src,
      0, null, null)
  }

  protected def hostToDeviceCopy(ct: ClassTag[_])(src: Pointer, dest: cl_mem,
                                                      elementCount: Long,
                                              offset: Int = 0): Unit = {
    val length = elementCount * baseSize2(ct)
    clEnqueueWriteBuffer(context.getOpenCLQueue, dest, CL_TRUE, offset, length, src,
      0, null, null)
  }

  protected def deviceToHostCopy[V: ClassTag](src: cl_mem, dest: Pointer, elementCount: Long, offset: Long = 0): Unit = {
    val length = elementCount * baseSize[V]
    val offsetInBytes = offset * baseSize[V]
    clEnqueueReadBuffer(context.getOpenCLQueue, src, CL_TRUE, offsetInBytes, length, dest, 0, null,
      null)
  }

  protected def deviceToDeviceCopy[V: ClassTag](src: cl_mem, dest: cl_mem, elementCount: Long,
                                           offset: Long = 0): Unit = {
    val length = elementCount * baseSize[V]
    val offsetInBytes = offset * baseSize[V]
    clEnqueueCopyBuffer(context.getOpenCLQueue, src, dest, 0, offsetInBytes, length, 0, null, null)
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
  val < = Value("lth")
  val <= = Value("leq")
  val > = Value("gth")
  val >= = Value("geq")
  val == = Value("eql")
  val != = Value("neq")
}

object DataPosition extends Enumeration {
  val HOST = Value("host")
  val DEVICE = Value("device")

}