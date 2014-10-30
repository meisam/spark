package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class GpuJoinPartition[T <: Product : ClassTag, T2 <: Product : ClassTag : TypeTag,
U: ClassTag : TypeTag]
(context: OpenCLContext, columnTypes: Array[String], rightPartition: GpuPartition[T2],
 joinColIndexLeft: Int, joinColIndexRight: Int, capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {

  def buildHashTable() = {

    val hsize = (1 to 31).map(1 << _).filter(_ >= rightPartition.size).head

    // Number of primary keys for each hash value
    val gpu_hashNum = createReadWriteBuffer[Int](hsize)

    val memSetKernel = clCreateKernel(context.program, "cl_memset_int", null)
    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array(hsize)))

    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)

    clEnqueueNDRangeKernel(context.getOpenCLQueue, memSetKernel, 1, null, global_work_size, local_work_size, 0,
      null, null)

    val threadNum = globalSize

    // Per thread
    val gpu_count = createReadWriteBuffer[Int](threadNum)
    // Per thread move to join on GPU
    val gpu_resPsum = createReadWriteBuffer[Int](threadNum)

    // prefix sum for gpu_hashNum
    val gpu_psum = createReadWriteBuffer[Int](hsize)

    //twice as big as # of keys
    val gpu_bucket = createReadWriteBuffer[Int](2 * rightPartition.size) //TODO change [Int[ to [U]

    val gpu_psum1 = createReadWriteBuffer[Int](hsize)

    val columnPosition = rightPartition.dataPosition(joinColIndexRight)

    val gpu_dim = columnPosition match {
      case DataPosition.HOST =>
        createReadBuffer[Int](rightPartition.size) // TODO should this be [Int] or [U]
      case DataPosition.DEVICE =>
        throw new NotImplementedError("DataPosition.DEVICE is not supported!")
      case _ =>
        throw new NotImplementedError("Unknown DataPosition type!")

    }
    val rightColumn: Array[U] = rightPartition.getColumn[U](joinColIndexRight)
    //TODO why this should be blocking
    hostToDeviceCopy[U](pointer(rightColumn), gpu_dim, /*CL_TRUE,*/ rightPartition.size)

    val countHashKernel = clCreateKernel(context.program, "count_hash_num", null)
    clSetKernelArg(countHashKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_dim))
    clSetKernelArg(countHashKernel, 1, Sizeof.cl_long,
      pointer(Array[Long](rightPartition.size.toLong)))
    clSetKernelArg(countHashKernel, 2, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(countHashKernel, 3, Sizeof.cl_int, pointer(Array[Long](hsize)))
    clEnqueueNDRangeKernel(context.queue, countHashKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)
    scanImpl(gpu_hashNum, hsize, gpu_psum)

    deviceToDeviceCopy[Int](gpu_psum, gpu_psum1, hsize)

    val buildHashTableKernel = clCreateKernel(context.program, "build_hash_table", null)
    clSetKernelArg(buildHashTableKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_dim))
    clSetKernelArg(buildHashTableKernel, 1, Sizeof.cl_long, pointer(Array[Long](rightPartition.size.toLong)))
    clSetKernelArg(buildHashTableKernel, 2, Sizeof.cl_mem, Pointer.to(gpu_psum1))
    clSetKernelArg(buildHashTableKernel, 3, Sizeof.cl_mem, Pointer.to(gpu_bucket))
    clSetKernelArg(buildHashTableKernel, 4, Sizeof.cl_int, pointer(Array[Int](hsize)))
    clEnqueueNDRangeKernel(context.queue, buildHashTableKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    if (columnPosition == DataPosition.HOST)
      clReleaseMemObject(gpu_dim)

    clReleaseMemObject(gpu_psum1)
  }

  def joinOnGpu(): Int = {
    -1
  }

  def join(): Int = {
    buildHashTable

    joinOnGpu
  }

  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    val resultSize = join
  }

}
