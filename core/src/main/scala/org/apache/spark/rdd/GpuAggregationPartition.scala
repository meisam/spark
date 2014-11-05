package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof, cl_mem}

import scala.reflect.ClassTag

class GpuAggregationPartition[T <: Product : ClassTag]
(context: OpenCLContext, columnTypes: Array[String], groupByColumnIndexes: Array[Int],
 aggregations: Array[AggregationOperation.Value], capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {
  //
  def aggregate(): Unit = {

    def align(offset: Int): Int = {
      offset + (if (offset % 4 == 0) 0 else 4 - (offset % 4))
    }

    val cpuOffsets: Array[Int] = columnTypes.map(c => baseSize2(scalaTypeOf(c))).scanLeft(0)(
    {
      case (sum: Int, x: Int) => align(x) + sum
    })

    val totalSize = cpuOffsets.last

    val gpuContent = createReadBuffer[Byte](totalSize) // [Byte] because everything is in bytes

    var offsetIndex = 0

    val tupleCount = this.size // TODO this.size or parent.size?
    intData.foreach { column =>
      hostToDeviceCopy[Int](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    longData.foreach { column =>
      hostToDeviceCopy[Long](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    floatData.foreach { column =>
      hostToDeviceCopy[Float](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    doubleData.foreach { column =>
      hostToDeviceCopy[Double](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    booleanData.foreach { column =>
      hostToDeviceCopy[Boolean](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    charData.foreach { column =>
      hostToDeviceCopy[Char](pointer(column), gpuContent, tupleCount, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    stringData.foreach { column =>
      hostToDeviceCopy[Char](pointer(column), gpuContent, tupleCount * MAX_STRING_SIZE, cpuOffsets(offsetIndex))
      offsetIndex += 1
    }

    val gpuOffsets = createReadBuffer[Int](this.typeNameString().length)
    hostToDeviceCopy[Int](pointer(cpuOffsets), gpuOffsets, this.typeNameString().length)

    val gpuGbType = createReadBuffer[Int](groupByColumnIndexes.length)
    hostToDeviceCopy(pointer(groupByColumnIndexes), gpuGbType, groupByColumnIndexes.length)

    val gpuGbSize = createReadBuffer[Int](groupByColumnIndexes.length)
    val groupBySize = groupByColumnIndexes.map(columnTypes(_)).map(scalaTypeOf).map(baseSize2)
      .scanLeft(0: Int)({ case (sum, x) => sum + x}) // TODO or sum + align(x)

    hostToDeviceCopy[Int](pointer(groupBySize), gpuGbSize, groupByColumnIndexes.length)

    val gpuGbKey = createReadWriteBuffer[Int](this.size) //TODO  this.size or parent.size

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

    val buildGroupByKeyKernel = clCreateKernel(context.program,
      "build_groupby_key", null)
    clSetKernelArg(buildGroupByKeyKernel, 0, Sizeof.cl_mem, Pointer.to(gpuContent))
    clSetKernelArg(buildGroupByKeyKernel, 1, Sizeof.cl_mem, Pointer.to(gpuOffsets))
    clSetKernelArg(buildGroupByKeyKernel, 2, Sizeof.cl_int,
      pointer(Array[Int](groupByColumnIndexes.length)))
    clSetKernelArg(buildGroupByKeyKernel, 3, Sizeof.cl_mem, Pointer.to(gpuGbIndex))
    clSetKernelArg(buildGroupByKeyKernel, 4, Sizeof.cl_mem, Pointer.to(gpuGbType))
    clSetKernelArg(buildGroupByKeyKernel, 5, Sizeof.cl_mem, Pointer.to(gpuGbSize))
    clSetKernelArg(buildGroupByKeyKernel, 6, Sizeof.cl_long,
      pointer(Array[Int](columnTypes.length)))
    clSetKernelArg(buildGroupByKeyKernel, 7, Sizeof.cl_mem, Pointer.to(gpuGbKey))
    clSetKernelArg(buildGroupByKeyKernel, 8, Sizeof.cl_mem, Pointer.to(gpu_hashNum))

    clEnqueueNDRangeKernel(context.queue, buildGroupByKeyKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    clReleaseMemObject(gpuGbType)
    clReleaseMemObject(gpuGbSize)
    clReleaseMemObject(gpuGbIndex)


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

    val gpu_psum = createReadWriteBuffer[Int](HASH_SIZE)

    scanImpl(gpu_hashNum, HASH_SIZE, gpu_psum)

    clReleaseMemObject(gpuGbCount)
    clReleaseMemObject(gpu_hashNum)
  }


  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    this.aggregate()
  }
}

object AggregationOperation extends Enumeration {

  def count = Value("count")

  def min = Value("min")

  def max = Value("max")

  def sum = Value("sum")

  def avg = Value("avg")
