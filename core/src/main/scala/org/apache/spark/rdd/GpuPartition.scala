package org.apache.spark.rdd

import org.apache.spark.{Partition, Logging}
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._
import scala.reflect.ClassTag
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.universe.{ TypeTag, typeOf }
import scala.reflect.runtime.{ universe => ru }
import java.nio.Buffer
import java.nio.ByteBuffer
import java.nio.ShortBuffer
import java.nio.IntBuffer
import java.nio.LongBuffer
import java.nio.FloatBuffer
import java.nio.DoubleBuffer
import java.nio.CharBuffer
import java.io.FileInputStream
import java.nio.file.Files
import java.io.File
import java.nio.ByteOrder

class GpuPartition[T <: Product: TypeTag](context: OpenCLContext, idx: Int, val capacity: Int)
  extends Partition with Serializable with Logging {

  type JavaType = JavaUniverse#Type

  def columnTypes = typeOf[T] match {
    case ru.TypeRef(tpe, sym, typeArgs) => typeArgs
    case _ => throw new NotImplementedError("Unknown type %s".format(typeOf[T]))
  }

  def MAX_STRING_SIZE: Int = 1 << 7

  def HASH_SIZE = 131072

  var size = 0

  val byteData = Array.fill[ByteBuffer](columnTypes.count(_ =:= TypeTag.Byte.tpe)) {
    ByteBuffer.wrap(Array.ofDim[Byte](capacity))
  }
  val shortData = Array.fill[ShortBuffer](columnTypes.count(_ =:= TypeTag.Short.tpe)) {
    ShortBuffer.wrap(Array.ofDim[Short](capacity))
  }
  val intData = Array.fill[IntBuffer](columnTypes.count(_ =:= TypeTag.Int.tpe)) {
    IntBuffer.wrap(Array.ofDim[Int](capacity))
  }
  val longData = Array.fill[LongBuffer](columnTypes.count(_ =:= TypeTag.Long.tpe)) {
    LongBuffer.wrap(Array.ofDim[Long](capacity))
  }
  val floatData = Array.fill[FloatBuffer](columnTypes.count(_ =:= TypeTag.Float.tpe)) {
    FloatBuffer.wrap(Array.ofDim[Float](capacity))
  }
  val doubleData = Array.fill[DoubleBuffer](columnTypes.count(_ =:= TypeTag.Double.tpe)) {
    DoubleBuffer.wrap(Array.ofDim[Double](capacity))
  }
  val booleanData = Array.fill[ByteBuffer](columnTypes.count(_ =:= TypeTag.Boolean.tpe)) {
    ByteBuffer.wrap(Array.ofDim[Byte](capacity))
  }
  val charData = Array.fill[CharBuffer](columnTypes.count(_ =:= TypeTag.Char.tpe)) {
    CharBuffer.wrap(Array.ofDim[Char](capacity))
  }
  val stringData = Array.fill[CharBuffer](columnTypes.count(_ =:= ColumnarTypes.StringTypeTag.tpe)) {
    CharBuffer.wrap(Array.ofDim[Char](capacity * MAX_STRING_SIZE))
  }

  def inferBestWorkGroupSize(): Unit = {
    this.localSize = if (size == 0) 1 else math.min(BLOCK_SIZE, size)
    this.globalSize = localSize * math.min(1 + (size - 1) / localSize, BLOCK_SIZE)
  }

  def fillFromFiles(paths: Array[String]): Unit = {
    assert(paths.length == columnTypes.size, { " %d file paths but only %d columns".format(paths.length, columnTypes.size) })
    columnTypes.zip(paths).zipWithIndex.foreach({
      case ((colType, path), colIndex) =>

        val startDiskReadTime = System.nanoTime()

        val columnData = ByteBuffer.wrap(Files.readAllBytes(new File(path).toPath()))
        columnData.order(ByteOrder.LITTLE_ENDIAN)
        val totalTupleNum = columnData.getLong()

        val tuplesInBlock = columnData.getLong()
        assert(tuplesInBlock == totalTupleNum, { f"tuplesInBlock != totalTupleNum ($tuplesInBlock != $totalTupleNum )" })

        val blockSize = columnData.getLong()
        assert(blockSize == totalTupleNum * baseSize(colType), { f"blockSize != totalTupleNum * sizeof($colType) ($blockSize != $totalTupleNum * sizeof($colType))" })

        val blockTotal = columnData.getInt()
        assert(blockTotal == 1, { f"blockTotal != 1 ($blockTotal != 1)" })

        val blockId = columnData.getInt()
        assert(blockId == 0, { f"blockId != 0 ($blockId!= 0)" })

        val format = columnData.getInt()
        assert(format == 3, { f"format != 3 ($format != 3)" })

        val paddingLength = 4060
        columnData.position(columnData.position + paddingLength)
        assert(columnData.position == 0x1000)

        val restData = columnData.slice()
        restData.order(ByteOrder.LITTLE_ENDIAN)
        val remaining = restData.remaining()
        assert(remaining == blockSize, { f"remaining != blockSize ($remaining != $blockSize)" })

        if (colType =:= TypeTag.Byte.tpe) {
          val convertBuffer = new Array[Byte](totalTupleNum.toInt)
          restData.get(convertBuffer)
          byteData(toTypeAwareColumnIndex(colIndex)) = ByteBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Short.tpe) {
          val convertBuffer = new Array[Short](totalTupleNum.toInt)
          restData.asShortBuffer().get(convertBuffer)
          shortData(toTypeAwareColumnIndex(colIndex)) = ShortBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Int.tpe) {
          val convertBuffer = new Array[Int](totalTupleNum.toInt)
          restData.asIntBuffer().get(convertBuffer)
          intData(toTypeAwareColumnIndex(colIndex)) = IntBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Long.tpe) {
          val convertBuffer = new Array[Long](totalTupleNum.toInt)
          restData.asLongBuffer().get(convertBuffer)
          longData(toTypeAwareColumnIndex(colIndex)) = LongBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Float.tpe) {
          val convertBuffer = new Array[Float](totalTupleNum.toInt)
          restData.asFloatBuffer().get(convertBuffer)
          floatData(toTypeAwareColumnIndex(colIndex)) = FloatBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Double.tpe) {
          val convertBuffer = new Array[Double](totalTupleNum.toInt)
          restData.asDoubleBuffer().get(convertBuffer)
          doubleData(toTypeAwareColumnIndex(colIndex)) = DoubleBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Boolean.tpe) {
          val convertBuffer = new Array[Byte](totalTupleNum.toInt)
          restData.get(convertBuffer)
          booleanData(toTypeAwareColumnIndex(colIndex)) = ByteBuffer.wrap(convertBuffer)
        } else if (colType =:= TypeTag.Char.tpe) {
          val convertBuffer = new Array[Char](totalTupleNum.toInt)
          restData.asCharBuffer().get(convertBuffer)
          charData(toTypeAwareColumnIndex(colIndex)) = CharBuffer.wrap(convertBuffer)
        } else if (colType =:= ColumnarTypes.StringTypeTag.tpe) {
          val convertBuffer = new Array[Char](totalTupleNum.toInt * MAX_STRING_SIZE)
          restData.asCharBuffer().get(convertBuffer)
          stringData(toTypeAwareColumnIndex(colIndex)) = CharBuffer.wrap(convertBuffer)
        } else {
          throw new NotImplementedError("Unknown type %s".format(colType))
        }

        val endDiskReadTime = System.nanoTime()

        val diskReadTime = endDiskReadTime - startDiskReadTime

        this.size = totalTupleNum.toInt
        inferBestWorkGroupSize

        context.diskReadTime += diskReadTime

    })
  }

  def fill(iter: Iterator[T]): Unit = {
    size = 0
    val values: Iterator[T] = iter.take(capacity)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        size = rowIndex + 1
        v.productIterator.zipWithIndex.foreach {
          case (p, colIndex) =>
            if (columnTypes(colIndex) =:= TypeTag.Byte.tpe) {
              byteData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Byte])
            } else if (columnTypes(colIndex) =:= TypeTag.Short.tpe) {
              shortData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Short])
            } else if (columnTypes(colIndex) =:= TypeTag.Int.tpe) {
              intData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Int])
            } else if (columnTypes(colIndex) =:= TypeTag.Long.tpe) {
              longData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Long])
            } else if (columnTypes(colIndex) =:= TypeTag.Float.tpe) {
              floatData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Float])
            } else if (columnTypes(colIndex) =:= TypeTag.Double.tpe) {
              doubleData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Double])
            } else if (columnTypes(colIndex) =:= TypeTag.Boolean.tpe) {
              booleanData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, if (p.asInstanceOf[Boolean]) 1 else 0)
            } else if (columnTypes(colIndex) =:= TypeTag.Char.tpe) {
              charData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Char])
            } else if (columnTypes(colIndex) =:= ColumnarTypes.StringTypeTag.tpe) {
              val str = p.toString
              str.getChars(0, Math.min(MAX_STRING_SIZE, str.length),
                stringData(toTypeAwareColumnIndex(colIndex)).array, rowIndex * MAX_STRING_SIZE)
            } else {
              throw new NotImplementedError("Unknown type %s".format(columnTypes(colIndex)))
            }
        }
    }
    inferBestWorkGroupSize
  }

  def getStringData(typeAwareColumnIndex: Int, rowIndex: Int): String = {
    val offset = rowIndex * MAX_STRING_SIZE
    val str = new String(stringData(typeAwareColumnIndex).array(), offset, MAX_STRING_SIZE)
    str.trim()
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

    val values = columnTypes.zipWithIndex.map({
      case (colType, colIndex) =>
        if (colType =:= TypeTag.Byte.tpe) {
          byteData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Short.tpe) {
          shortData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Int.tpe) {
          intData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Long.tpe) {
          longData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Float.tpe) {
          floatData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Double.tpe) {
          doubleData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= TypeTag.Boolean.tpe) {
          booleanData(toTypeAwareColumnIndex(colIndex)).get(rowIndex) != 0
        } else if (colType =:= TypeTag.Char.tpe) {
          charData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType =:= ColumnarTypes.StringTypeTag.tpe) {
          getStringData(toTypeAwareColumnIndex(colIndex), rowIndex)
        } else {
          throw new NotImplementedError("Unknown type %s".format(colType))
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

  def preallocBlockSums(maxNumElements: Int): Array[cl_mem] = {
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
    val g_scanBlockSums = new Array[cl_mem](level)
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
    g_scanBlockSums
  }

  def prescanArrayRecursive(outArray: cl_mem, inArray: cl_mem, numElements: Int, level: Int,
    same: Int, g_scanBlockSums: Array[cl_mem]) {

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
      prescanArrayRecursive(g_scanBlockSums(level), g_scanBlockSums(level), numBlocks, level + 1, 1, g_scanBlockSums)
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

  def deallocBlockSums(g_scanBlockSums: Array[cl_mem]) {
    g_scanBlockSums.foreach(clReleaseMemObject(_))
    g_numEltsAllocated = 0
    g_numLevelsAllocated = 0
  }

  def prescanArray(outArray: cl_mem, inArray: cl_mem, numElements: Int, g_scanBlockSums: Array[cl_mem]) {
    prescanArrayRecursive(outArray, inArray, numElements, 0, 0, g_scanBlockSums)
  }

  def scanImpl(d_input: cl_mem, rLen: Int, d_output: cl_mem) {
    val g_scanBlockSums = preallocBlockSums(rLen)
    prescanArray(d_output, d_input, rLen, g_scanBlockSums)
    deallocBlockSums(g_scanBlockSums)
  }

  def toArray[X: TypeTag](value: X): Array[X] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    implicit val xClassTag = ClassTag[X](mirror.runtimeClass(typeOf[X]))
    Array[X](value)
  }

  def pointer[T: TypeTag](values: Array[T]): Pointer = {

    if (typeOf[T] =:= typeOf[Byte]) {
      Pointer.to(values.asInstanceOf[Array[Byte]])
    } else if (typeOf[T] =:= typeOf[Short]) {
      Pointer.to(values.asInstanceOf[Array[Short]])
    } else if (typeOf[T] =:= typeOf[Int]) {
      Pointer.to(values.asInstanceOf[Array[Int]])
    } else if (typeOf[T] =:= typeOf[Long]) {
      Pointer.to(values.asInstanceOf[Array[Long]])
    } else if (typeOf[T] =:= typeOf[Float]) {
      Pointer.to(values.asInstanceOf[Array[Float]])
    } else if (typeOf[T] =:= typeOf[Double]) {
      Pointer.to(values.asInstanceOf[Array[Double]])
    } else if (typeOf[T] =:= typeOf[Char]) {
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else if (typeOf[T] =:= typeOf[Char]) {
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else if (typeOf[T] =:= ColumnarTypes.StringTypeTag.tpe) {
      Pointer.to(values.asInstanceOf[Array[Char]])
    } else {
      throw new NotImplementedError("Cannot create a pointer to an array of %s.".format(
        typeOf[T].toString))
    }
  }

  def releaseCol(col: cl_mem) {
    clReleaseMemObject(col)
  }

  def javaTypeToTypeTag(tpe: JavaType): TypeTag[_] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] defined in $mirror cannot be migrated to $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def getColumn[V: TypeTag](columnIndex: Int): Buffer = {
    val typeAwareColumnIndex = toTypeAwareColumnIndex(columnIndex)

    implicitly[TypeTag[V]] match {
      case TypeTag.Byte => byteData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Short => shortData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Char => charData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Int => intData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Long => longData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Float => floatData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Double => doubleData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case TypeTag.Boolean => booleanData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case ColumnarTypes.StringTypeTag => stringData(typeAwareColumnIndex).asInstanceOf[Buffer]
      case _ => throw new NotImplementedError("Unknown type %s".format(implicitly[TypeTag[V]]))
    }
  }

  def extractType[V: TypeTag]: JavaType = {
    typeOf[V] match {
      case ru.TypeRef(tpe, sym, typeArgs) => tpe
      case _ => throw new NotImplementedError("Unknown type %s".format(typeOf[V]))
    }
  }

  def baseSize[V: TypeTag]: Int = {
    implicitly[TypeTag[V]] match {
      case TypeTag.Byte => Sizeof.cl_char
      case TypeTag.Short => Sizeof.cl_short
      case TypeTag.Char => Sizeof.cl_char2
      case TypeTag.Int => Sizeof.cl_int
      case TypeTag.Long => Sizeof.cl_long
      case TypeTag.Float => Sizeof.cl_float
      case TypeTag.Double => Sizeof.cl_double
      case TypeTag.Boolean => Sizeof.cl_char
      case ColumnarTypes.StringTypeTag => Sizeof.cl_char2 * MAX_STRING_SIZE
      case _ => throw new NotImplementedError("Unknown type %s".format(implicitly[TypeTag[V]]))
    }
  }

  def baseSize(ct: JavaType): Int = {
    if (ct == TypeTag.Byte.tpe) {
      Sizeof.cl_char
    } else if (ct == TypeTag.Short.tpe) {
      Sizeof.cl_short
    } else if (ct == TypeTag.Char.tpe) {
      Sizeof.cl_short
    } else if (ct == TypeTag.Int.tpe) {
      Sizeof.cl_int
    } else if (ct == TypeTag.Long.tpe) {
      Sizeof.cl_long
    } else if (ct == TypeTag.Float.tpe) {
      Sizeof.cl_float
    } else if (ct == TypeTag.Double.tpe) {
      Sizeof.cl_double
    } else if (ct == TypeTag.Boolean.tpe) {
      Sizeof.cl_char
    } else if (ct == ColumnarTypes.StringTypeTag.tpe) {
      Sizeof.cl_char * MAX_STRING_SIZE
    } else {
      throw new NotImplementedError("Unknown type %s".format(ct.toString()))
    }
  }

  def typeNameString[V: TypeTag](): String = {
    typeOf[V].toString.toLowerCase
  }

  def dataPosition(columnIndex: Int) = {
    //TODO For now, data is always stored on device (cpu)
    DataPosition.HOST
  }

  protected def createReadBuffer[V: TypeTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.context, CL_MEM_READ_ONLY, size, null, null)
  }

  protected def createReadBuffer(elementCount: Int, columnType: JavaType): cl_mem = {
    val size = elementCount * baseSize(columnType)
    clCreateBuffer(context.context, CL_MEM_READ_ONLY, size, null, null)
  }

  protected def createReadWriteBuffer[V: TypeTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.context, CL_MEM_READ_WRITE, size, null, null)
  }

  protected def createWriteBuffer[V: TypeTag](elementCount: Int): cl_mem = {
    val size = elementCount * baseSize[V]
    clCreateBuffer(context.context, CL_MEM_WRITE_ONLY, size, null, null)
  }

  protected def createWriteBuffer(size: Long): cl_mem = {
    clCreateBuffer(context.context, CL_MEM_WRITE_ONLY, size, null, null)
  }

  protected def hostToDeviceCopy[V: TypeTag](src: Pointer, dest: cl_mem, elementCount: Long,
    offset: Int = 0): Unit = {
    val length = elementCount * baseSize[V]
    val startTime = System.nanoTime()
    clEnqueueWriteBuffer(context.queue, dest, CL_TRUE, offset, length, src, 0, null, null)
    val endTime = System.nanoTime()
    val copyToGpuTime = endTime - startTime
    context.pciTransferTime += copyToGpuTime
    context.pciTransferBytes += length
  }

  protected def hostToDeviceCopy[V: TypeTag](src: Buffer, dest: cl_mem, elementCount: Long,
    offset: Int): Unit = {
    val length = elementCount * baseSize[V]
    val scrBuffer = Pointer.to(src)
    val startTime = System.nanoTime()
    clEnqueueWriteBuffer(context.queue, dest, CL_TRUE, offset, length, scrBuffer, 0, null, null)
    val endTime = System.nanoTime()
    val copyToGpuTime = endTime - startTime
    context.pciTransferTime += copyToGpuTime
    context.pciTransferBytes += length
  }

  protected def deviceToHostCopy[V: TypeTag](src: cl_mem, dest: Pointer, elementCount: Long, offset: Long = 0): Unit = {
    val length = elementCount * baseSize[V]
    val offsetInBytes = offset * baseSize[V]
    val startTime = System.nanoTime()
    clEnqueueReadBuffer(context.queue, src, CL_TRUE, offsetInBytes, length, dest, 0, null,
      null)
    val endTime = System.nanoTime()
    val copyFromGpuTime = endTime - startTime
    context.pciTransferTime += copyFromGpuTime
    context.pciTransferBytes += length
  }

  protected def deviceToHostCopy[V: TypeTag](src: cl_mem, dest: Buffer, elementCount: Long, offset: Long): Unit = {
    val length = elementCount * baseSize[V]
    val offsetInBytes = offset * baseSize[V]
    val destPointer = Pointer.to(dest)
    val startTime = System.nanoTime()
    clEnqueueReadBuffer(context.queue, src, CL_TRUE, offsetInBytes, length, destPointer, 0,
      null, null)
    val endTime = System.nanoTime()
    val copyFromGpuTime = endTime - startTime
    context.pciTransferTime += copyFromGpuTime
    context.pciTransferBytes += length
  }

  protected def deviceToDeviceCopy[V: TypeTag](src: cl_mem, dest: cl_mem, elementCount: Long,
    offset: Long = 0): Unit = {
    val length = elementCount * baseSize[V]
    val offsetInBytes = offset * baseSize[V]
    clEnqueueCopyBuffer(context.queue, src, dest, 0, offsetInBytes, length, 0, null, null)
  }

  def release {
  }

  def debugGpuBuffer[V: TypeTag: ClassTag](buffer: cl_mem, size: Int, msg: String, quiet: Boolean = true) {
    if (!quiet) {
      val tempBuffer = new Array[V](size)
      deviceToHostCopy[V](buffer, pointer[V](tempBuffer), size, 0)
      println("%s = \n%s".format(msg, tempBuffer.mkString(" ,")))
    }
  }

  val POW_2_S: IndexedSeq[Long] = (0 to 100).map(_.toLong).map(1L << _)
  val BLOCK_SIZE: Int = 256
  val NUM_BANKS: Int = 16
  val LOG_NUM_BANKS: Int = 4

  var g_numEltsAllocated: Int = 0
  var g_numLevelsAllocated: Int = 0

  var globalSize = 0

  var localSize = 0

  var resCount = 0

//  /**
//   * Get the split's index within its parent RDD
//   */
//  override def hashCode(): Int = 41 * (41 + this.idxrddId) + idx

  override val index: Int = idx
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

object ColumnarTypes extends IndexedSeq[ru.Type] {

  val StringTypeTag = ru.typeTag[String]

  private val ALL_TYPES: IndexedSeq[ru.Type] = IndexedSeq(
    TypeTag.Byte.tpe,
    TypeTag.Short.tpe,
    TypeTag.Int.tpe,
    TypeTag.Long.tpe,
    TypeTag.Float.tpe,
    TypeTag.Double.tpe,
    TypeTag.Boolean.tpe,
    TypeTag.Char.tpe,
    StringTypeTag.tpe)

  def getIndex(t: ru.Type): Int = {
    ALL_TYPES.indexOf(t)
  }

  override def length: Int = ALL_TYPES.length

  override def apply(idx: Int): ru.Type = ALL_TYPES(idx)
}

