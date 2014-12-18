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

class GpuAggregationPartition[T <: Product: TypeTag](context: OpenCLContext, parentPartition: GpuPartition[T],
  groupByColumnIndexes: Array[Int], aggregations: Array[GroupByExp], capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def aggregate(iterator: Iterator[T]): Unit = {

    println("groupBy column idnexes = %s".format(groupByColumnIndexes.mkString(",")))

    def align(offset: Int): Int = {
      offset + (if (offset % 4 == 0) 0 else 4 - (offset % 4))
    }

    val tupleCount = parentPartition.size

    val cpuOffsets: Array[Int] = columnTypes.map(baseSize(_) * tupleCount).scanLeft(0)(
      {
        case (sum: Int, x: Int) => align(x) + sum
      }).toArray[Int]

    val totalSize = cpuOffsets.last

    val gpuContent = createReadWriteBuffer[Byte](totalSize) // [Byte] because everything is in bytes

    var offsetIndex = 0

    parentPartition.columnTypes.zipWithIndex.foreach {
      case (columnType, columnIndex) =>
        implicit val columnTypeTag = javaTypeToTypeTag(columnType)
        val column = parentPartition.getColumn(columnIndex)(columnTypeTag)
        hostToDeviceCopy[Byte](column, gpuContent, tupleCount * baseSize(columnType), cpuOffsets(offsetIndex))
        offsetIndex += 1
    }

    val gpuContentResults = new Array[Byte](totalSize)
    deviceToHostCopy[Byte](gpuContent, Pointer.to(gpuContentResults), totalSize, 0)

    val gpuOffsets = createReadBuffer[Int](cpuOffsets.length)
    hostToDeviceCopy[Int](pointer(cpuOffsets), gpuOffsets, cpuOffsets.length)

    val gbType: Array[Int] = groupByColumnIndexes.map(i => columnTypes(i)).map(t => ColumnarTypes.getIndex(t)).toIterator.toArray

    println("gb column types = %s".format(gbType.mkString(",")))

    val gpuGbType = createReadBuffer[Int](gbType.length)
    hostToDeviceCopy[Int](pointer(gbType), gpuGbType, gbType.length)

    val gpuGbSize = createReadBuffer[Int](groupByColumnIndexes.length)
    val groupBySize: Array[Int] = groupByColumnIndexes.map(columnTypes(_)).map(baseSize(_))
      .scanLeft(0: Int)({ case (sum, x) => sum + align(x) }).splitAt(1)._2.toArray
    println("groupBy sizes = %s".format(groupBySize.mkString(",")))

    hostToDeviceCopy[Int](pointer(groupBySize), gpuGbSize, groupByColumnIndexes.length)

    val gpuGbKey = createReadWriteBuffer[Int](parentPartition.size)

    val gpuGbIndex = createReadBuffer[Int](groupByColumnIndexes.length)
    hostToDeviceCopy[Int](pointer(groupByColumnIndexes), gpuGbIndex, groupByColumnIndexes.length)

    val gpu_hashNum = createReadWriteBuffer[Int](HASH_SIZE)

    val memSetKernel = clCreateKernel(context.program, "cl_memset_int", null)

    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](HASH_SIZE)))

    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)
    println("groupbyColNum = %d".format(groupByColumnIndexes.length))

    val buildGroupByKeyKernel = clCreateKernel(context.program, "build_groupby_key", null)
    clSetKernelArg(buildGroupByKeyKernel, 0, Sizeof.cl_mem, Pointer.to(gpuContent))
    clSetKernelArg(buildGroupByKeyKernel, 1, Sizeof.cl_mem, Pointer.to(gpuOffsets))
    clSetKernelArg(buildGroupByKeyKernel, 2, Sizeof.cl_int,
      pointer(Array[Int](groupByColumnIndexes.length)))
    clSetKernelArg(buildGroupByKeyKernel, 3, Sizeof.cl_mem, Pointer.to(gpuGbIndex))
    clSetKernelArg(buildGroupByKeyKernel, 4, Sizeof.cl_mem, Pointer.to(gpuGbType))
    clSetKernelArg(buildGroupByKeyKernel, 5, Sizeof.cl_mem, Pointer.to(gpuGbSize))
    clSetKernelArg(buildGroupByKeyKernel, 6, Sizeof.cl_long,
      pointer(Array[Int](parentPartition.size)))
    clSetKernelArg(buildGroupByKeyKernel, 7, Sizeof.cl_mem, Pointer.to(gpuGbKey))
    clSetKernelArg(buildGroupByKeyKernel, 8, Sizeof.cl_mem, Pointer.to(gpu_hashNum))

    clEnqueueNDRangeKernel(context.queue, buildGroupByKeyKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    val gpuGbKeyResults = new Array[Int](parentPartition.size)
    deviceToHostCopy[Int](gpuGbKey, Pointer.to(gpuGbKeyResults), parentPartition.size, 0)
    printf("gpuGbKeyResults = %s\n", gpuGbKeyResults.take(100).mkString(","))

    val gpuHashNumResults = new Array[Int](HASH_SIZE)
    deviceToHostCopy[Int](gpu_hashNum, Pointer.to(gpuHashNumResults), HASH_SIZE, 0)
    printf("gpuHashNumResults = %s\n", gpuHashNumResults.zipWithIndex.filter(_._1 != 0).mkString(","))

    val gpuGbCount = createReadWriteBuffer[Int](1)
    hostToDeviceCopy[Int](pointer(Array[Int](0)), gpuGbCount, 1)

    val countGroupNumKernel = clCreateKernel(context.program, "count_group_num", null);
    clSetKernelArg(countGroupNumKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(countGroupNumKernel, 1, Sizeof.cl_int, pointer(Array[Int](HASH_SIZE)))
    clSetKernelArg(countGroupNumKernel, 2, Sizeof.cl_mem, Pointer.to(gpuGbCount))
    clEnqueueNDRangeKernel(context.queue, countGroupNumKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    val gbCount = Array[Int](1)

    deviceToHostCopy[Int](gpuGbCount, pointer(gbCount), 1)

    println("Groupby count = %s".format(gbCount.mkString(",")))

    val gpu_psum = createReadWriteBuffer[Int](HASH_SIZE)

    scanImpl(gpu_hashNum, HASH_SIZE, gpu_psum)

    this.size = gbCount.head

    //  
    // @gpuGbExp is the mathExp in each groupBy Expession
    // @mathexp stores the math exp for for the group Expession that has two operands
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

    val gpuResult = createReadWriteBuffer[Byte](totalSize)
    val resOffset: Array[Long] = columnTypes.map(x => align(baseSize(x)).toLong).scan(0L)(_ + _).toArray
    val gpuResOffset = createReadWriteBuffer[Long](columnTypes.length)
    hostToDeviceCopy[Long](Pointer.to(resOffset), gpuResOffset, columnTypes.length, 0)

    val gpuGbColNum = columnTypes.length

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

    clEnqueueNDRangeKernel(context.queue, agggregationKernel, 1, null, global_work_size, local_work_size, 0, null, null)

    clReleaseMemObject(gpuGbKey);
    clReleaseMemObject(gpu_psum);

    /*

    for(int i=0; i<res->totalAttr;i++){
        res->content[i] = (char *)clCreateBuffer(context->context,CL_MEM_READ_WRITE, res->attrSize[i]*res->tupleNum, NULL, &error); 
        res->dataPos[i] = GPU;
        res->attrTotalSize[i] = res->tupleNum * res->attrSize[i];
        clEnqueueCopyBuffer(context->queue, gpuResult, (cl_mem)res->content[i], resOffset[i],0, res->attrSize[i] * res->tupleNum, 0,0,0);
    }

    free(resOffset);
    free(cpuOffset);

    clFinish(context->queue);
    clReleaseMemObject(gpuContent);
    clReleaseMemObject(gpuGbType);
    clReleaseMemObject(gpuGbSize);
    clReleaseMemObject(gpuResult);
    clReleaseMemObject(gpuOffset);
    clReleaseMemObject(gpuResOffset);
    clReleaseMemObject(gpuGbExp);
    clReleaseMemObject(gpuFunc);

    */

    clReleaseMemObject(gpuGbType)
    clReleaseMemObject(gpuGbSize)
    clReleaseMemObject(gpuGbIndex)

    clReleaseMemObject(gpuGbCount)
    clReleaseMemObject(gpu_hashNum)
  }

  override def fill(iter: Iterator[T]): Unit = {
    parentPartition.inferBestWorkGroupSize
    this.globalSize = parentPartition.globalSize
    this.localSize = parentPartition.localSize
    this.aggregate(iter)
  }
}

object AggregationOperation extends Enumeration {
  type AggregationOperation = Value
  val min = Value("MIN")
  val max = Value("MAX")
  val count = Value("COUNT")
  val sum = Value("SUM")
  val avg = Value("AVG")
}

class GroupByExp(val aggFunc: AggregationOperation.Value, val mathExp: MathExp) {
  override def toString() = {
    f"function = $aggFunc, mathExp = $mathExp"
  }
}

object MathOp extends Enumeration {
  type MathOp = Value
  val PLUS, MINU, MULTIPLY, DIVIDE, NOOP = Value
}

class MathExp(op: MathOp.Value, opNum: Int, val leftExp: MathExp, val rightExp: MathExp, opType: MathOp.Value, opValue: Int) {

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