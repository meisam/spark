package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{ Pointer, Sizeof }
import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.universe.TypeTag
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.jocl.cl_mem
import scala.reflect.ClassTag

class GpuAggregationPartition[T <: Product: TypeTag, TP <: Product: TypeTag](
  context: OpenCLContext, parentPartition: GpuPartition[TP],
  aggregations: Array[AggregationExp], capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def aggregate(iterator: Iterator[TP]): Unit = {
    parentPartition.inferBestWorkGroupSize
    this.globalSize = parentPartition.globalSize
    this.localSize = parentPartition.localSize

    def align(offset: Int): Int = {
      offset + (if (offset % 4 == 0) 0 else 4 - (offset % 4))
    }

    val gbColumnIndexes = aggregations.filter(agg => agg.aggFunc == AggregationOperation.groupBy).map { agg =>
      {
        assert(agg != null, { "agg is null" })
        assert(agg.mathExp != null, { "agg.mathExp is null" })
        assert(agg.mathExp.op == MathOp.NOOP, { "agg.mathExp operation should be NOOP" })
        assert(agg.mathExp.opType == MathOperationType.column, { "agg.mathExp.opType is not column" })
        agg.mathExp.opValue
      }
    }

    println("groupBy column idnexes = %s".format(gbColumnIndexes.mkString(",")))

    val tupleCount = parentPartition.size

    val cpuOffsets: Array[Long] = parentPartition.columnTypes.map(baseSize(_) * tupleCount).scanLeft(0L)(
      {
        case (sum: Long, x: Int) => align(x).toLong + sum
      }).toArray[Long]

    val totalSize = cpuOffsets.last.toInt

    val gpuContent = createReadWriteBuffer[Byte](totalSize) // [Byte] because everything is in bytes

    parentPartition.columnTypes.zipWithIndex.foreach {
      case (columnType, columnIndex) =>
        implicit val columnTypeTag = javaTypeToTypeTag(columnType)
        val column = parentPartition.getColumn(columnIndex)(columnTypeTag)
        hostToDeviceCopy[Byte](column, gpuContent, tupleCount * baseSize(columnType), cpuOffsets(columnIndex).toInt)
    }

    val gpuOffsets = createReadBuffer[Long](cpuOffsets.length)
    hostToDeviceCopy[Long](pointer(cpuOffsets), gpuOffsets, cpuOffsets.length)
    debugGpuBuffer[Long](gpuOffsets, cpuOffsets.length, "gpuOffsets (before)")

    val gbType: Array[Int] = gbColumnIndexes.map(i => columnTypes(i)).map(t => ColumnarTypes.getIndex(t)).toIterator.toArray

    println("gb column types = %s".format(gbType.mkString(",")))

    val gpuGbType = createReadBuffer[Int](gbType.length)
    hostToDeviceCopy[Int](pointer(gbType), gpuGbType, gbType.length)

    val gpuGbSize = createReadBuffer[Int](gbColumnIndexes.length)
    val groupBySize: Array[Int] = gbColumnIndexes.map(columnTypes(_)).map(baseSize(_))
      .scanLeft(0: Int)({ case (sum, x) => sum + align(x) }).splitAt(1)._2.toArray
    println("groupBy sizes = %s".format(groupBySize.mkString(",")))

    hostToDeviceCopy[Int](pointer(groupBySize), gpuGbSize, gbColumnIndexes.length)

    val gpuGbKey = createReadWriteBuffer[Int](parentPartition.size)

    val gpuGbIndex = createReadBuffer[Int](gbColumnIndexes.length)
    hostToDeviceCopy[Int](pointer(gbColumnIndexes), gpuGbIndex, gbColumnIndexes.length)

    val gpu_hashNum = createReadWriteBuffer[Int](HASH_SIZE)

    val memSetKernel = clCreateKernel(context.program, "cl_memset_int", null)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)

    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](HASH_SIZE)))

    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpuGbKey))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](parentPartition.size)))

    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    debugGpuBuffer[Int](gpuGbKey, parentPartition.size, "gpuGbKey before")

    val buildGroupByKeyKernel = clCreateKernel(context.program, "build_groupby_key", null)
    clSetKernelArg(buildGroupByKeyKernel, 0, Sizeof.cl_mem, Pointer.to(gpuContent))
    clSetKernelArg(buildGroupByKeyKernel, 1, Sizeof.cl_mem, Pointer.to(gpuOffsets))
    clSetKernelArg(buildGroupByKeyKernel, 2, Sizeof.cl_int,
      pointer(Array[Int](gbColumnIndexes.length)))
    clSetKernelArg(buildGroupByKeyKernel, 3, Sizeof.cl_mem, Pointer.to(gpuGbIndex))
    clSetKernelArg(buildGroupByKeyKernel, 4, Sizeof.cl_mem, Pointer.to(gpuGbType))
    clSetKernelArg(buildGroupByKeyKernel, 5, Sizeof.cl_mem, Pointer.to(gpuGbSize))
    clSetKernelArg(buildGroupByKeyKernel, 6, Sizeof.cl_long,
      pointer(Array[Long](parentPartition.size)))
    clSetKernelArg(buildGroupByKeyKernel, 7, Sizeof.cl_mem, Pointer.to(gpuGbKey))
    clSetKernelArg(buildGroupByKeyKernel, 8, Sizeof.cl_mem, Pointer.to(gpu_hashNum))

    println("global_work_size = %s".format(global_work_size.mkString(",")))
    println("local_work_size = %s".format(local_work_size.mkString(",")))
    println(f"total size = $totalSize")

    clEnqueueNDRangeKernel(context.queue, buildGroupByKeyKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    println("parentPartition.size = %s".format(parentPartition.size))

    debugGpuBuffer[Byte](gpuContent, totalSize, "gpuContent")

    debugGpuBuffer[Long](gpuOffsets, cpuOffsets.length, "gpuOffsets (after)")

    debugGpuBuffer[Int](gpuGbIndex, gbColumnIndexes.length, "gpuGbIndex")

    debugGpuBuffer[Int](gpuGbType, gbType.length, "gpuGbType")

    debugGpuBuffer[Int](gpuGbSize, gbColumnIndexes.length, "gpuGbSize")

    debugGpuBuffer[Int](gpuGbKey, parentPartition.size, "gpuGbKey (after)")

    // next line prints too many results
    // debugGpuBuffer[Int](gpu_hashNum,HASH_SIZE, "gpu_hashNum")

    val gpuGbKeyResults = new Array[Int](parentPartition.size)
    deviceToHostCopy[Int](gpuGbKey, Pointer.to(gpuGbKeyResults), parentPartition.size, 0)

    val gpuHashNumResults = new Array[Int](HASH_SIZE)
    deviceToHostCopy[Int](gpu_hashNum, Pointer.to(gpuHashNumResults), HASH_SIZE, 0)
    printf("gpuHashNumResults = %s\n", gpuHashNumResults.zipWithIndex.filter(_._1 != 0).mkString(","))

    val gpuGbCount = createReadWriteBuffer[Int](1)
    hostToDeviceCopy[Int](pointer(Array[Int](0)), gpuGbCount, 1)

    val countGroupNumKernel = clCreateKernel(context.program, "count_group_num", null);
    clSetKernelArg(countGroupNumKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(countGroupNumKernel, 1, Sizeof.cl_int, pointer(Array[Int](HASH_SIZE)))
    clSetKernelArg(countGroupNumKernel, 2, Sizeof.cl_mem, Pointer.to(gpuGbCount))
    println("count_group_num 1")
    clEnqueueNDRangeKernel(context.queue, countGroupNumKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    val gbCount = Array[Int](1)

    println("count_group_num 2")
    deviceToHostCopy[Int](gpuGbCount, pointer(gbCount), 1)

    println("Groupby count = %s".format(gbCount.mkString(",")))

    val gpu_psum = createReadWriteBuffer[Int](HASH_SIZE)

    scanImpl(gpu_hashNum, HASH_SIZE, gpu_psum)

    this.size = gbCount.head

    //  
    // @gpuGbExp is the mathExp in each groupBy Expression
    // @mathexp stores the math exp for for the group Expression that has two operands
    // The reason that we need two variables instead of one is that OpenCL doesn't support pointer to pointer
    //

    var offset = 0

    val gpuGbExpBuffer = ByteBuffer.wrap(new Array[Byte](MathExp.size * columnTypes.length))
    val mathExpBuffer = ByteBuffer.wrap(new Array[Byte](2 * MathExp.size * columnTypes.length))
    val cpuFuncs = aggregations.map(_.aggFunc.id)

    println("all aggregations = %s".format(aggregations.mkString(",")))

    aggregations.foreach { gbExp =>
      if (gbExp.mathExp == null) {
        gpuGbExpBuffer.position(gpuGbExpBuffer.position + MathExp.size)
        mathExpBuffer.position(mathExpBuffer.position + MathExp.size)
        mathExpBuffer.position(mathExpBuffer.position + MathExp.size)
        println("skipping gbExpr  write")

      } else {
        gbExp.mathExp.writeTo(gpuGbExpBuffer)
        println("writing gb math exp%s".format(gbExp.mathExp))

        if (gbExp.mathExp.leftExp == null) {
          mathExpBuffer.position(mathExpBuffer.position + MathExp.size)
          println("skipping left exp")

        } else {
          gbExp.mathExp.leftExp.writeTo(mathExpBuffer)
          println("writing left math exp%s".format(gbExp.mathExp.leftExp))
        }

        if (gbExp.mathExp.rightExp == null) {
          mathExpBuffer.position(mathExpBuffer.position + MathExp.size)
          println("skipping right exp")
        } else {
          gbExp.mathExp.rightExp.writeTo(mathExpBuffer)
          println("writing rightmath exp%s".format(gbExp.mathExp.rightExp))
        }
      }
    }

    val gpuGbExp = createReadBuffer[Byte](MathExp.size * columnTypes.length)
    val gpuMathExp = createReadBuffer[Byte](2 * MathExp.size * columnTypes.length)
    val gpuFunc = createReadBuffer[Int](columnTypes.length)

    println("gpuGbExpBuffer = %s".format(gpuGbExpBuffer.array().mkString(",")))

    println("mathExpBuffer = %s".format(mathExpBuffer.array().mkString(",")))

    hostToDeviceCopy[Byte](Pointer.to(gpuGbExpBuffer), gpuGbExp, MathExp.size * columnTypes.length);
    hostToDeviceCopy[Byte](Pointer.to(mathExpBuffer), gpuMathExp, 2 * MathExp.size * columnTypes.length)
    hostToDeviceCopy[Int](Pointer.to(cpuFuncs), gpuFunc, columnTypes.length)

    val resultOffsets: Array[Long] = columnTypes.map(baseSize(_) * this.size).scanLeft(0L)(
      {
        case (sum: Long, x: Int) => align(x).toLong + sum
      }).toArray[Long]

    val resultTotalSize = resultOffsets.last.toInt

    println(f"resultTotalSize = $resultTotalSize")

    val gpuResult = createReadWriteBuffer[Byte](resultTotalSize)

    val memSetFloatKernel = clCreateKernel(context.program, "cl_memset_nan", null)
    clSetKernelArg(memSetFloatKernel, 0, Sizeof.cl_mem, Pointer.to(gpuResult))
    aggregations.zip(resultOffsets).foreach {
      case (agg, offset) =>
        if ((agg.aggFunc == AggregationOperation.min) || (agg.aggFunc == AggregationOperation.max)) {
          clSetKernelArg(memSetFloatKernel, 1, Sizeof.cl_int, pointer(Array[Int](this.size)))
          clSetKernelArg(memSetFloatKernel, 2, Sizeof.cl_int, pointer(Array[Long](offset / 4)))
          debugGpuBuffer[Float](gpuResult, resultTotalSize / 4, "gpuResult (float) (before NaN-ing)")
          debugGpuBuffer[Int](gpuResult, resultTotalSize / 4, "gpuResult (int) (before NaN-ing)")
          clEnqueueNDRangeKernel(context.queue, memSetFloatKernel, 1, null, global_work_size,
            local_work_size, 0, null, null)
          debugGpuBuffer[Float](gpuResult, resultTotalSize / 4, "gpuResult (float) (after NaN-ing)")
          debugGpuBuffer[Int](gpuResult, resultTotalSize / 4, "gpuResult (int) (after NaN-ing)")
        } else {
          println("Zero-ing offset = %d with agg type %s".format(offset, agg.aggFunc.toString()))
          // we actually do not need zero them again because we already have done it.
        }
    }

    val gpuResOffset = createReadWriteBuffer[Long](columnTypes.length)
    hostToDeviceCopy[Long](Pointer.to(resultOffsets), gpuResOffset, columnTypes.length, 0)

    val gpuGbColNum = columnTypes.length //FIXME is this correct?

    val agggregationKernel = clCreateKernel(context.program, "agg_cal", null)
    clSetKernelArg(agggregationKernel, 0, Sizeof.cl_mem, Pointer.to(gpuContent))
    clSetKernelArg(agggregationKernel, 1, Sizeof.cl_mem, Pointer.to(gpuOffsets))
    clSetKernelArg(agggregationKernel, 2, Sizeof.cl_int, pointer[Int](Array(gpuGbColNum)))
    clSetKernelArg(agggregationKernel, 3, Sizeof.cl_mem, Pointer.to(gpuGbExp))
    clSetKernelArg(agggregationKernel, 4, Sizeof.cl_mem, Pointer.to(gpuMathExp))
    clSetKernelArg(agggregationKernel, 5, Sizeof.cl_mem, Pointer.to(gpuGbType))
    clSetKernelArg(agggregationKernel, 6, Sizeof.cl_mem, Pointer.to(gpuGbSize))
    clSetKernelArg(agggregationKernel, 7, Sizeof.cl_long, pointer[Long](Array(parentPartition.size.toLong)))
    clSetKernelArg(agggregationKernel, 8, Sizeof.cl_mem, Pointer.to(gpuGbKey))
    clSetKernelArg(agggregationKernel, 9, Sizeof.cl_mem, Pointer.to(gpu_psum))
    clSetKernelArg(agggregationKernel, 10, Sizeof.cl_mem, Pointer.to(gpuResult))
    clSetKernelArg(agggregationKernel, 11, Sizeof.cl_mem, Pointer.to(gpuResOffset))
    clSetKernelArg(agggregationKernel, 12, Sizeof.cl_mem, Pointer.to(gpuFunc))

    println("MathExp.size * columnTypes.length = %d".format(MathExp.size * columnTypes.length))
    debugGpuBuffer[Byte](gpuContent, totalSize, "gpuContent (before agg_cal)")
    debugGpuBuffer[Long](gpuOffsets, cpuOffsets.length, "gpuOffsets (before agg_cal)")
    debugGpuBuffer[Byte](gpuGbExp, MathExp.size * columnTypes.length, "gpuGbExp (before agg_cal)")
    debugGpuBuffer[Byte](gpuMathExp, 2 * MathExp.size * columnTypes.length, "gpuMathExp (before agg_cal)")
    debugGpuBuffer[Int](gpuGbType, gbType.length, "gpuGbType (before agg_cal)")
    debugGpuBuffer[Int](gpuGbSize, gbColumnIndexes.length, "gpuGbSize (before agg_cal)")
    debugGpuBuffer[Int](gpuGbKey, parentPartition.size, "gpuGbKey (before agg_cal)")
    // next line prints too much if uncommented 
    //    debugGpuBuffer[Int](gpu_psum, HASH_SIZE, "gpu_psum")
    debugGpuBuffer[Long](gpuResOffset, columnTypes.length, "gpuResOffset (before agg_cal)")
    debugGpuBuffer[Int](gpuFunc, columnTypes.length, "gpuFunc (before agg_cal)")

    println("aggregations = %s".format(aggregations.mkString(" ...\n,... ")))
    println("cpuFuncs = %s".format(cpuFuncs.mkString(",")))

    debugGpuBuffer[Byte](gpuResult, resultTotalSize, "gpuResult (byte) (before agg_cal)")
    debugGpuBuffer[Int](gpuResult, resultTotalSize / 4, "gpuResult (int) (before agg_cal)")
    debugGpuBuffer[Float](gpuResult, resultTotalSize / 4, "gpuResult (float) (before agg_cal)")

    clEnqueueNDRangeKernel(context.queue, agggregationKernel, 1, null, global_work_size, local_work_size, 0, null, null)

    debugGpuBuffer[Byte](gpuResult, resultTotalSize, "gpuResult (byte) (after agg_cal)")
    debugGpuBuffer[Int](gpuResult, resultTotalSize / 4, "gpuResult (int) (after agg_cal)")
    debugGpuBuffer[Float](gpuResult, resultTotalSize / 4, "gpuResult (float) (after agg_cal)")

    clReleaseMemObject(gpuGbKey);
    clReleaseMemObject(gpu_psum);

    columnTypes.zipWithIndex.foreach {
      case (columnType, columnIndex) =>
        implicit val columTypeTag = javaTypeToTypeTag(columnType)
        val column = getColumn(toTypeAwareColumnIndex(columnIndex))(columTypeTag)
        deviceToHostCopy[Byte](gpuResult, column, gpuGbColNum * baseSize(columnType), resultOffsets(columnIndex))
    }

    clFinish(context.queue)
    clReleaseMemObject(gpuContent);
    clReleaseMemObject(gpuResult);
    clReleaseMemObject(gpuResOffset);
    clReleaseMemObject(gpuGbExp);
    clReleaseMemObject(gpuFunc);
    clReleaseMemObject(gpuGbType)
    clReleaseMemObject(gpuGbSize)
    clReleaseMemObject(gpuGbIndex)

    clReleaseMemObject(gpuGbCount)
    clReleaseMemObject(gpu_hashNum)
  }

  def debugGpuBuffer[V: TypeTag: ClassTag](buffer: cl_mem, size: Int, msg: String, quiet: Boolean = true) {
    if (!quiet) {
      val tempBuffer = new Array[V](size)
      deviceToHostCopy[V](buffer, pointer[V](tempBuffer), size, 0)
      println("%s = \n%s".format(msg, tempBuffer.mkString(" ,")))
    }
  }
}

object AggregationOperation extends Enumeration {
  type AggregationOperation = Value
  val groupBy = Value("G.B.")
  val min = Value("MIN")
  val max = Value("MAX")
  val count = Value("COUNT")
  val sum = Value("SUM")
  val avg = Value("AVG")
}

object MathOperationType extends Enumeration {
  type MathOperationType = Value
  val column = Value("COLUMN")
  val const = Value("CONST")
}
class AggregationExp(val aggFunc: AggregationOperation.Value, val mathExp: MathExp) {
  override def toString() = {
    f"function = $aggFunc, mathExp = [$mathExp]"
  }
}

object MathOp extends Enumeration {
  type MathOp = Value
  // This order should exactly match the order in kernel.cl file otherwise nothing works
  val NOOP = Value("NOOP")
  val PLUS = Value("PLUS")
  val MINU = Value("MINUS")
  val MULTIPLY = Value("MULTIPLY")
  val DIVIDE = Value("DIVIDE")
}

class MathExp(val op: MathOp.Value, opNum: Int, val leftExp: MathExp, val rightExp: MathExp, val opType: MathOperationType.Value, val opValue: Int) {

  def writeTo(out: ByteBuffer): Unit = {
    out.order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(op.id)
    out.putInt(opNum)
    out.putInt(if (leftExp == null) 0 else MathExp.size)
    out.putInt(if (rightExp == null) 0 else MathExp.size)
    out.putInt(opType.id)
    out.putInt(opValue)
  }

  override def toString() = {
    "op= %s, operandCount=%d, leftExp = %s, rightExp = %s, opType = %s, opValue=%d".format(
      op, /*________*/ opNum, /**/ leftExp, /**/ rightExp, /**/ opType, /**/ opValue)
  }
}

object MathExp {
  def size = 4 * (1 + 1 + 2 + 1 + 1) // 4 bytes per word *( Words in Int, Int, Long, Int, Int)
}