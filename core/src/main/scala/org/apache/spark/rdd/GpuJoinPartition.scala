package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.runtime.universe.TypeTag

class GpuJoinPartition[T <: Product : TypeTag, TL <: Product : TypeTag, TR <: Product : TypeTag,
U: TypeTag]
(context: OpenCLContext, leftPartition: GpuPartition[TL], rightPartition: GpuPartition[TR],
 joinColIndexLeft: Int, joinColIndexRight: Int, capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def columnFromLeftPartition(columnIndex: Int): Boolean = {
    columnIndex < leftPartition.columnTypes.length
  }

  def toRightTableIndex(columnIndex: Int) = {
    if (columnFromLeftPartition(columnIndex))
      throw new IllegalArgumentException("%d is from the left table!".format(columnIndex))

    if (columnIndex >= columnTypes.length)
      throw new IllegalArgumentException("%d is too big to be from the right table!".format(columnIndex))
    
    if (columnIndex < joinColIndexRight + leftPartition.columnTypes.length )
      columnIndex - leftPartition.columnTypes.length
    else
      columnIndex - leftPartition.columnTypes.length + 1 // add 1 to skip counting join column twice
    
  }

  def buildHashTable() = {

    val hsize = (1 to 31).map(1 << _).filter(_ >= rightPartition.size).head

    // Number of primary keys for each hash value
    val gpu_hashNum = createReadWriteBuffer[Int](hsize)

    val memSetIntKernel = clCreateKernel(context.program, "cl_memset_int", null)
    clSetKernelArg(memSetIntKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(memSetIntKernel, 1, Sizeof.cl_int, pointer(Array(hsize)))

    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)

    clEnqueueNDRangeKernel(context.getOpenCLQueue, memSetIntKernel, 1, null, global_work_size, local_work_size, 0,
      null, null)

    val threadNum = globalSize

    // Per thread
    val gpu_count = createReadWriteBuffer[Int](threadNum)
    // Per thread move to join on GPU
    val gpu_resPsum = createReadWriteBuffer[Int](threadNum)

    // prefix sum for gpu_hashNum
    val gpu_psum = createReadWriteBuffer[Int](hsize)

    //twice as big as # of keys
    val gpu_bucket = createReadWriteBuffer[U](2 * rightPartition.size)

    val gpu_psum1 = createReadWriteBuffer[Int](hsize) // This buffer is not needed. Delete it

    val columnPosition = rightPartition.dataPosition(joinColIndexRight)

    val gpu_dim = columnPosition match {
      case DataPosition.HOST =>
        createReadBuffer[U](rightPartition.size)
      case DataPosition.DEVICE =>
        throw new NotImplementedError("DataPosition.DEVICE is not supported!")
      case _ =>
        throw new NotImplementedError("Unknown DataPosition type!")

    }
    val rightColumn = rightPartition.getColumn[U](joinColIndexRight)
    //TODO why this should be blocking
    hostToDeviceCopy[U](rightColumn, gpu_dim, /*CL_TRUE,*/ rightPartition.size, 0)

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
    /*  }

      def joinOnGpu(): Int = {
    */

    val dataPos = dataPosition(joinColIndexLeft)

    //  long foreignKeySize = jNode->leftTable->attrTotalSize[jNode->leftKeyIndex];
    //  long filterSize = jNode->leftTable->attrSize[jNode->leftKeyIndex] * jNode->leftTable->tupleNum;

    val gpu_fact: cl_mem = dataPos match {
      case DataPosition.HOST =>
        createReadBuffer[U](leftPartition.size)
      case DataPosition.DEVICE =>
        throw new NotImplementedError("DataPosition.DEVICE is not supported!")
      case _ =>
        throw new NotImplementedError("Unknown DataPosition type!")
    }
    hostToDeviceCopy[U](leftPartition.getColumn[U](joinColIndexLeft), gpu_fact, leftPartition.size, 0)

    val gpuFactFilter = createReadWriteBuffer[U](leftPartition.size)

    val kernelName = "cl_memset_%s".format(typeNameString[U])
    val memSetKernel = clCreateKernel(context.program, kernelName, null)
    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpuFactFilter))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](leftPartition.size)))

    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    val countJoinKernel = clCreateKernel(context.program, "count_join_result", null)
    clSetKernelArg(countJoinKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_hashNum))
    clSetKernelArg(countJoinKernel, 1, Sizeof.cl_mem, Pointer.to(gpu_psum))
    clSetKernelArg(countJoinKernel, 2, Sizeof.cl_mem, Pointer.to(gpu_bucket))
    clSetKernelArg(countJoinKernel, 3, Sizeof.cl_mem, Pointer.to(gpu_fact))
    clSetKernelArg(countJoinKernel, 4, Sizeof.cl_long, pointer(Array[Long](leftPartition.size.toLong)))
    clSetKernelArg(countJoinKernel, 5, Sizeof.cl_mem, Pointer.to(gpu_count))
    clSetKernelArg(countJoinKernel, 6, Sizeof.cl_mem, Pointer.to(gpuFactFilter))
    clSetKernelArg(countJoinKernel, 7, Sizeof.cl_int, pointer(Array[Int](hsize)))
    clEnqueueNDRangeKernel(context.queue, countJoinKernel, 1, null, global_work_size, local_work_size, 0, null, null)

    clReleaseMemObject(gpu_hashNum)

    val gpuCountTotal = Array[Int](0)

    deviceToHostCopy[Int](gpu_count, pointer(gpuCountTotal), 1, threadNum - 1)

    scanImpl(gpu_count, threadNum, gpu_resPsum)

    val gpuResSumTotal = Array[Int](0)
    deviceToHostCopy[Int](gpu_resPsum, pointer(gpuResSumTotal), 1, threadNum - 1)

    val joinResultCount = gpuCountTotal.head + gpuResSumTotal.head
    this.size = joinResultCount
    printf("[INFO]joinNum %,d =  gpuCountTotal.head + gpuResSumTotal.head = %,d +  %,d\n",
      joinResultCount, gpuCountTotal.head, gpuResSumTotal.head)
    clReleaseMemObject(gpu_fact)

    clReleaseMemObject(gpu_bucket)

    if (joinResultCount > 0) {
      columnTypes.zipWithIndex.foreach {
        case (columnType, columnIndex) =>
          implicit val columnTypeTag = javaTypeToTypeTag(columnType)
          val colSize = baseSize(columnType)

          val gpu_result = createReadBuffer(joinResultCount, columnType)
  
          if (columnFromLeftPartition(columnIndex)) {
            val leftColumn = leftPartition.getColumn(columnIndex)(columnTypeTag)
            val gpu_fact = createReadWriteBuffer(leftPartition.size)(columnTypeTag)
            hostToDeviceCopy(leftColumn, gpu_fact, leftPartition.size, 0)(columnTypeTag)
            val javaTypeName = typeNameString()(columnTypeTag)
            val joinFactKernelName = f"join_fact_$javaTypeName"
            val joinFactKernel = clCreateKernel(context.program, joinFactKernelName, null)
            clSetKernelArg(joinFactKernel,0,Sizeof.cl_mem, Pointer.to(gpu_resPsum))
            clSetKernelArg(joinFactKernel,1,Sizeof.cl_mem, Pointer.to(gpu_fact))
            clSetKernelArg(joinFactKernel,2,Sizeof.cl_int, pointer(toArray[Int](baseSize(columnType))))
            clSetKernelArg(joinFactKernel,3,Sizeof.cl_long, pointer(toArray[Long](leftPartition.size)))
            clSetKernelArg(joinFactKernel,4,Sizeof.cl_mem, Pointer.to(gpuFactFilter))
            clSetKernelArg(joinFactKernel,5,Sizeof.cl_mem, Pointer.to(gpu_result))
            clEnqueueNDRangeKernel(context.queue, joinFactKernel, 1, null, 
                global_work_size, local_work_size, 0, null, null)

            clReleaseMemObject(gpu_fact)
          } else {
            val rightColumnIndex = toRightTableIndex(columnIndex)
            if (rightColumnIndex != joinColIndexRight) {
            	rightPartition.columnTypes.zipWithIndex.map(_._2 + leftPartition.columnTypes.length)
              val column = rightPartition.getColumn(rightColumnIndex)(columnTypeTag)
              val sizeInBytes = joinResultCount * baseSize(columnType)
  
              val gpu_fact = createReadWriteBuffer(sizeInBytes)(columnTypeTag)
              hostToDeviceCopy(column, gpu_fact, rightPartition.size, 0)(columnTypeTag)
              
  
              val javaTypeName = typeNameString()(columnTypeTag)
              val joinKernelName = f"join_dim_$javaTypeName"
              val joinDimKernel = clCreateKernel(context.program, joinKernelName, null)
              clSetKernelArg(joinDimKernel, 0, Sizeof.cl_mem, Pointer.to(gpu_resPsum))
              clSetKernelArg(joinDimKernel, 1, Sizeof.cl_mem, Pointer.to(gpu_fact))
              clSetKernelArg(joinDimKernel, 2, Sizeof.cl_int, pointer(toArray[Int](baseSize(columnType))))
              clSetKernelArg(joinDimKernel, 3, Sizeof.cl_long, pointer(toArray[Long](leftPartition.size)))
              clSetKernelArg(joinDimKernel, 4, Sizeof.cl_mem, Pointer.to(gpuFactFilter))
              clSetKernelArg(joinDimKernel, 5, Sizeof.cl_mem, Pointer.to(gpu_result))
              clEnqueueNDRangeKernel(context.queue, joinDimKernel, 1, null,
                global_work_size, local_work_size, 0, null, null)
  
              clReleaseMemObject(gpu_fact)
            }
          }
          val resultColumn = this.getColumn(columnIndex)(columnTypeTag)
          deviceToHostCopy(gpu_result, resultColumn, joinResultCount, 0)(columnTypeTag)

          clReleaseMemObject(gpu_result)
      }
    }
    clReleaseMemObject(gpuFactFilter)
    clReleaseMemObject(gpu_resPsum)
    clReleaseMemObject(gpu_psum)
    clReleaseMemObject(gpu_count)
    joinResultCount
  }

  def join(): Int = {
    this.globalSize = leftPartition.globalSize
    this.localSize = leftPartition.localSize
    buildHashTable

  }

  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    val resultSize = join
  }

}
