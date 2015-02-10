package org.apache.spark.rdd

import java.io._
import java.nio.file.Files
import java.nio.{Buffer, ByteBuffer, ByteOrder, CharBuffer, DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer, ShortBuffer}

import org.apache.spark.Logging
import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.ClassTag
import scala.reflect.api.Universe
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

class GpuPartition[T <: Product : TypeTag](context: OpenCLContext, val capacity: Int)
  extends Serializable with Logging {

  type JavaType = Universe#Type

  @transient private var _columnTypes: Array[TypeTag[_]] = _

  def columnTypes = {
    if (_columnTypes == null) {
      _columnTypes = (typeOf[T] match {
        case ru.TypeRef(tpe, sym, typeArgs) => typeArgs
        case _ => throw new NotImplementedError("Unknown type %s".format(typeOf[T]))
      }).map(x => javaTypeToTypeTag(x)).toArray
    }
    _columnTypes
  }

  def MAX_STRING_SIZE: Int = 1 << 7

  def HASH_SIZE = 131072

  var size = 0

  @transient private var _columnOffsets: Array[Int] = _

  def columnOffsets = {
    if (_columnOffsets == null) {
      _columnOffsets = columnTypes.map(t => baseSize(t) * capacity).scan(0)(_ + _).toArray
    }
    _columnOffsets
  }

  def byteData = {
    if (_byteData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Byte.tpe).map(_._2)
      _byteData = new Array[ByteBuffer](colIndexes.length)
      _byteData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _byteData(i) = ByteBuffer.allocateDirect(length)
      }
    }
    _byteData
  }

  def shortData = {
    if (_shortData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Short.tpe).map(_._2)
      _shortData = new Array[ShortBuffer](colIndexes.length)
      _shortData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _shortData(i) = ByteBuffer.allocateDirect(length).asShortBuffer()
      }
    }
    _shortData
  }

  def intData = {
    if (_intData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Int.tpe).map(_._2)
      _intData = new Array[IntBuffer](colIndexes.length)
      _intData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _intData(i) = ByteBuffer.allocateDirect(length).asIntBuffer()
      }
    }
    _intData
  }

  def longData = {
    if (_longData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Long.tpe).map(_._2)
      _longData = new Array[LongBuffer](colIndexes.length)
      _longData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _longData(i) = ByteBuffer.allocateDirect(length).asLongBuffer()
      }
    }
    _longData
  }

  def floatData = {
    if (_floatData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Float.tpe).map(_._2)
      _floatData = new Array[FloatBuffer](colIndexes.length)
      _floatData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _floatData(i) = ByteBuffer.allocateDirect(length).asFloatBuffer()
      }
    }
    _floatData
  }

  def doubleData = {
    if (_doubleData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Double.tpe).map(_._2)
      _doubleData = new Array[DoubleBuffer](colIndexes.length)
      _doubleData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _doubleData(i) = ByteBuffer.allocateDirect(length).asDoubleBuffer()
      }
    }
    _doubleData
  }

  def booleanData = {
    if (_booleanData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Boolean.tpe).map(_._2)
      _booleanData = new Array[ByteBuffer](colIndexes.length)
      _booleanData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _booleanData(i) = ByteBuffer.allocateDirect(length)
      }
    }
    _booleanData
  }

  def charData = {
    if (_charData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Char.tpe).map(_._2)
      _charData = new Array[CharBuffer](colIndexes.length)
      _charData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _charData(i) = ByteBuffer.allocateDirect(length).asCharBuffer()
      }
    }
    _charData
  }

  def stringData = {
    if (_stringData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= ColumnarTypes.StringTypeTag.tpe)
        .map(_._2)
      _stringData = new Array[CharBuffer](colIndexes.length)
      _stringData.indices.foreach { i =>
        val offset = columnOffsets(i)
        val length = columnOffsets(i + 1) - offset
        _stringData(i) = ByteBuffer.allocateDirect(length).asCharBuffer()
      }
    }
    _stringData
  }


  @transient var _byteData: Array[ByteBuffer] = null

  @transient var _shortData: Array[ShortBuffer] = null

  @transient var _intData: Array[IntBuffer] = null

  @transient var _longData: Array[LongBuffer] = null

  @transient var _floatData: Array[FloatBuffer] = null

  @transient var _doubleData: Array[DoubleBuffer] = null

  @transient var _booleanData: Array[ByteBuffer] = null

  @transient var _charData: Array[CharBuffer] = null

  @transient var _stringData: Array[CharBuffer] = null

  def inferBestWorkGroupSize(): Unit = {
    this.localSize = if (size == 0) 1 else math.min(BLOCK_SIZE, size)
    this.globalSize = localSize * math.min(1 + (size - 1) / localSize, BLOCK_SIZE)
  }

  def fillFromFiles(paths: Array[String]): Unit = {
    assert(paths.length == columnTypes.size, {
      " %d file paths but only %d columns".format(paths.length, columnTypes.size)
    })
    columnTypes.zip(paths).zipWithIndex.foreach({
      case ((colType, path), colIndex) =>

        val startDiskReadTime = System.nanoTime()

        val columnData = ByteBuffer.wrap(Files.readAllBytes(new File(path).toPath()))
        columnData.order(ByteOrder.LITTLE_ENDIAN)
        val totalTupleNum = columnData.getLong()

        val tuplesInBlock = columnData.getLong()
        assert(tuplesInBlock == totalTupleNum, {
          f"tuplesInBlock != totalTupleNum ($tuplesInBlock != $totalTupleNum )"
        })

        val blockSize = columnData.getLong()
        assert(blockSize == totalTupleNum * baseSize(colType), {
          f"blockSize != totalTupleNum * sizeof($colType) ($blockSize != $totalTupleNum * sizeof($colType))"
        })

        val blockTotal = columnData.getInt()
        assert(blockTotal == 1, {
          f"blockTotal != 1 ($blockTotal != 1)"
        })

        val blockId = columnData.getInt()
        assert(blockId == 0, {
          f"blockId != 0 ($blockId!= 0)"
        })

        val format = columnData.getInt()
        assert(format == 3, {
          f"format != 3 ($format != 3)"
        })

        val paddingLength = 4060
        columnData.position(columnData.position + paddingLength)
        assert(columnData.position == 0x1000)

        val restData = columnData.slice()
        restData.order(ByteOrder.LITTLE_ENDIAN)
        val remaining = restData.remaining()
        assert(remaining == blockSize, {
          f"remaining != blockSize ($remaining != $blockSize)"
        })

        if (colType.tpe =:= TypeTag.Byte.tpe) {
          val convertBuffer = new Array[Byte](totalTupleNum.toInt)
          restData.get(convertBuffer)
          byteData(toTypeAwareColumnIndex(colIndex)) = ByteBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Short.tpe) {
          val convertBuffer = new Array[Short](totalTupleNum.toInt)
          restData.asShortBuffer().get(convertBuffer)
          shortData(toTypeAwareColumnIndex(colIndex)) = ShortBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Int.tpe) {
          val convertBuffer = new Array[Int](totalTupleNum.toInt)
          restData.asIntBuffer().get(convertBuffer)
          intData(toTypeAwareColumnIndex(colIndex)) = IntBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Long.tpe) {
          val convertBuffer = new Array[Long](totalTupleNum.toInt)
          restData.asLongBuffer().get(convertBuffer)
          longData(toTypeAwareColumnIndex(colIndex)) = LongBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Float.tpe) {
          val convertBuffer = new Array[Float](totalTupleNum.toInt)
          restData.asFloatBuffer().get(convertBuffer)
          floatData(toTypeAwareColumnIndex(colIndex)) = FloatBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Double.tpe) {
          val convertBuffer = new Array[Double](totalTupleNum.toInt)
          restData.asDoubleBuffer().get(convertBuffer)
          doubleData(toTypeAwareColumnIndex(colIndex)) = DoubleBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Boolean.tpe) {
          val convertBuffer = new Array[Byte](totalTupleNum.toInt)
          restData.get(convertBuffer)
          booleanData(toTypeAwareColumnIndex(colIndex)) = ByteBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= TypeTag.Char.tpe) {
          val convertBuffer = new Array[Char](totalTupleNum.toInt)
          restData.asCharBuffer().get(convertBuffer)
          charData(toTypeAwareColumnIndex(colIndex)) = CharBuffer.wrap(convertBuffer)
        } else if (colType.tpe =:= ColumnarTypes.StringTypeTag.tpe) {
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
    val zeroCharBuffer = Array.fill[Char](MAX_STRING_SIZE)(0.toChar)

    size = 0
    val values: Iterator[T] = iter.take(capacity)
    values.zipWithIndex.foreach {
      case (v, rowIndex) =>
        size = rowIndex + 1
        v.productIterator.zipWithIndex.foreach {
          case (p, colIndex) =>
            if (columnTypes(colIndex).tpe =:= TypeTag.Byte.tpe) {
              byteData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Byte])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Short.tpe) {
              shortData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Short])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Int.tpe) {
              intData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Int])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Long.tpe) {
              longData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Long])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Float.tpe) {
              floatData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Float])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Double.tpe) {
              doubleData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Double])
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Boolean.tpe) {
              booleanData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, if (p.asInstanceOf[Boolean]) 1 else 0)
            } else if (columnTypes(colIndex).tpe =:= TypeTag.Char.tpe) {
              charData(toTypeAwareColumnIndex(colIndex)).put(rowIndex, p.asInstanceOf[Char])
            } else if (columnTypes(colIndex).tpe =:= ColumnarTypes.StringTypeTag.tpe) {
              val str = p.toString
              val offset = rowIndex * MAX_STRING_SIZE
              val charCount = Math.min(MAX_STRING_SIZE, str.length)
              val trailingExtraCharCount = MAX_STRING_SIZE - str.length
              val destBuffer = stringData(toTypeAwareColumnIndex(colIndex))
              str.getChars(0, charCount, reusedCharBuffer, 0)
              destBuffer.position(offset)
              destBuffer.put(reusedCharBuffer, 0, charCount)
              //Fill rest of it with zeros
              if (trailingExtraCharCount > 0) {
                destBuffer.put(zeroCharBuffer, 0, trailingExtraCharCount)
              }
            } else {
              throw new NotImplementedError("Unknown type %s".format(columnTypes(colIndex)))
            }
        }
    }
    inferBestWorkGroupSize
  }

  def getStringData(typeAwareColumnIndex: Int, rowIndex: Int): String = {
    val offset = rowIndex * MAX_STRING_SIZE
    val sourceBuffer = stringData(typeAwareColumnIndex)
    sourceBuffer.position(offset)
    sourceBuffer.get(reusedCharBuffer)
    val str = new String(reusedCharBuffer)
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
        if (colType.tpe =:= TypeTag.Byte.tpe) {
          byteData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Short.tpe) {
          shortData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Int.tpe) {
          intData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Long.tpe) {
          longData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Float.tpe) {
          floatData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Double.tpe) {
          doubleData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= TypeTag.Boolean.tpe) {
          booleanData(toTypeAwareColumnIndex(colIndex)).get(rowIndex) != 0
        } else if (colType.tpe =:= TypeTag.Char.tpe) {
          charData(toTypeAwareColumnIndex(colIndex)).get(rowIndex)
        } else if (colType.tpe =:= ColumnarTypes.StringTypeTag.tpe) {
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

    val columnType = implicitly[TypeTag[V]].tpe
    if (columnType =:= TypeTag.Byte.tpe) {
      byteData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Short.tpe) {
      shortData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Char.tpe) {
      charData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Int.tpe) {
      intData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Long.tpe) {
      longData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Float.tpe) {
      floatData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Double.tpe) {
      doubleData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= TypeTag.Boolean.tpe) {
      booleanData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else if (columnType =:= ColumnarTypes.StringTypeTag.tpe) {
      stringData(typeAwareColumnIndex).asInstanceOf[Buffer]
    } else {
      throw new NotImplementedError("Unknown type %s".format(implicitly[TypeTag[V]]))
    }
    }

  def baseSize[V: TypeTag]: Int = {
    val tt = implicitly[TypeTag[V]]
    if (tt.tpe =:= TypeTag.Byte.tpe) {
      Sizeof.cl_char
    } else if (tt.tpe =:= TypeTag.Short.tpe) {
      Sizeof.cl_short
    } else if (tt.tpe =:= TypeTag.Char.tpe) {
      Sizeof.cl_char2
    } else if (tt.tpe =:= TypeTag.Int.tpe) {
      Sizeof.cl_int
    } else if (tt.tpe =:= TypeTag.Long.tpe) {
      Sizeof.cl_long
    } else if (tt.tpe =:= TypeTag.Float.tpe) {
      Sizeof.cl_float
    } else if (tt.tpe =:= TypeTag.Double.tpe) {
      Sizeof.cl_double
    } else if (tt.tpe =:= TypeTag.Boolean.tpe) {
      Sizeof.cl_char
    } else if (tt.tpe =:= ColumnarTypes.StringTypeTag.tpe) {
      Sizeof.cl_char2 * MAX_STRING_SIZE
    }
    else {
      throw new NotImplementedError("Unknown type %s".format(implicitly[TypeTag[V]]))
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

  def BLOCK_SIZE: Int = 256

  def NUM_BANKS: Int = 16

  var g_numEltsAllocated: Int = 0
  var g_numLevelsAllocated: Int = 0

  var globalSize = 0

  var localSize = 0

  var resCount = 0

  // defined here to avoid frequent allocation and gc pressure.
  private var reusedCharBuffer = new Array[Char](MAX_STRING_SIZE)

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(columnTypes.size)
    columnOffsets.foreach { offset =>
      out.writeInt(offset)

    }
    columnTypes.foreach { colType =>
      val index = ColumnarTypes.getIndex(colType.tpe)
      out.writeInt(index)
    }
    out.writeInt(this.size)
    intData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size).foreach { i =>
        out.writeInt(buffer.get())
      }
    }

    longData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size).foreach { i =>
        out.writeLong(buffer.get())
      }
    }

    stringData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size * MAX_STRING_SIZE).foreach { i =>
        out.writeChar(buffer.get())
      }
    }

  }

  @scala.throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {
    reusedCharBuffer = new Array[Char](MAX_STRING_SIZE)
    val columnCounts = in.readInt()

    _columnOffsets = new Array[Int](columnCounts + 1)
    _columnOffsets.indices.foreach { i =>
      _columnOffsets(i) = in.readInt()
    }
    this._columnTypes = (0 until columnCounts).map { index =>
      val typeIndex = in.readInt()
      val columnType = ColumnarTypes(typeIndex)
      javaTypeToTypeTag(columnType)
    }.toArray

    this.size = in.readInt()
    intData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size).foreach { i =>
        buffer.put(in.readInt())
      }
    }
    longData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size).foreach { i =>
        buffer.put(in.readLong())
      }
    }
    stringData.foreach { buffer =>
      buffer.rewind()
      (0 until this.size * MAX_STRING_SIZE).foreach { i =>
        buffer.put(in.readChar())
      }
    }
  }
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
    ALL_TYPES.indexWhere(_ =:= t)
  }

  override def length: Int = ALL_TYPES.length

  override def apply(idx: Int): ru.Type = ALL_TYPES(idx)
}

