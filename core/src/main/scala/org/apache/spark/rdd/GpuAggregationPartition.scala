package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{ Pointer, Sizeof }

import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.universe.TypeTag

class GpuAggregationPartition[T <: Product: TypeTag](context: OpenCLContext, parentPartition: GpuPartition[T],
  groupByColumnIndexes: Array[Int], aggregations: Array[AggregationOperation.Value], capacity: Int)
    extends GpuPartition[T](context, capacity) {

  def aggregate(iterator: Iterator[T]): Unit = {

    def align(offset: Int): Int = {
      offset + (if (offset % 4 == 0) 0 else 4 - (offset % 4))
    }

    val tupleCount = parentPartition.size

    val cpuOffsets: Array[Int] = columnTypes.map(baseSize(_) * tupleCount).scanLeft(0)(
      {
        case (sum: Int, x: Int) => align(x) + sum
      }).toArray[Int]

    val totalSize = cpuOffsets.last


    val gpuContent = createReadBuffer[Byte](totalSize) // [Byte] because everything is in bytes

    var offsetIndex = 0
    
    
    parentPartition.columnTypes.zipWithIndex.foreach { case (columnType, columnIndex) =>
      implicit val columnTypeTag = javaTypeToTypeTag(columnType)
      val column = parentPartition.getColumn(columnIndex)(columnTypeTag)
      println(f"columnType($columnIndex) = $columnType")
      println(column.array.asInstanceOf[Array[_]].mkString(","))

      hostToDeviceCopy[Byte](column, gpuContent, tupleCount * baseSize(columnType), cpuOffsets(offsetIndex))
      offsetIndex += 1
    }
    
    val gpuContentResults = new Array[Int](totalSize)
    deviceToHostCopy[Byte](gpuContent, Pointer.to(gpuContentResults), totalSize, 0)
    printf("gpuContentResults = %s\n", gpuContentResults.mkString(","))
    

    val gpuOffsets = createReadBuffer[Int](cpuOffsets.length)
    hostToDeviceCopy[Int](pointer(cpuOffsets), gpuOffsets, cpuOffsets.length)

    val gpuGbType = createReadBuffer[Int](groupByColumnIndexes.length)
    hostToDeviceCopy[Int](pointer(groupByColumnIndexes), gpuGbType, groupByColumnIndexes
      .length)

    val gpuGbSize = createReadBuffer[Int](groupByColumnIndexes.length)
    val groupBySize: Array[Int] = groupByColumnIndexes.map(columnTypes(_)).map(baseSize(_))
      .scanLeft(0: Int)({ case (sum, x) => sum + x }).toIterator.toArray // TODO or sum + align(x)

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

    val buildGroupByKeyKernel = clCreateKernel(context.program, "build_groupby_key", null)
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

    val gpu_hashNumResults = new Array[Int](HASH_SIZE)
    deviceToHostCopy[Int](gpu_hashNum, Pointer.to(gpu_hashNumResults), HASH_SIZE, 0)
    printf("gpu_hashNumResults = %s\n", gpu_hashNumResults.zipWithIndex.filter(_._1 != 0).mkString(","))

    val gpuGbKeyResults = new Array[Int](parentPartition.size)
    deviceToHostCopy[Int](gpuGbKey, Pointer.to(gpuGbKeyResults), parentPartition.size, 0)
    printf("gpuGbKeyResults = %s\n", gpuGbKeyResults.take(100).mkString(","))
    
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
    
    println("Groupby count = %s".format(gbCount.mkString(",")) )

    val gpu_psum = createReadWriteBuffer[Int](HASH_SIZE)

    scanImpl(gpu_hashNum, HASH_SIZE, gpu_psum)

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

  def count = Value("count")

  def min = Value("min")

  def max = Value("max")

  def sum = Value("sum")

  def avg = Value("avg")
}
