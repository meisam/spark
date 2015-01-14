package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{ Pointer, Sizeof }
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import org.jocl.cl_mem

class GpuFilteredPartition[T <: Product: TypeTag, U: TypeTag](context: OpenCLContext, parent: GpuPartition[T], columnIndex: Int,
  operation: ComparisonOperation.Value, value: U, capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def filter() = {

    val tupleNum = parent.size
    gpuCol = createReadWriteBuffer[U](tupleNum)

    val col = parent.getColumn[U](columnIndex)
    hostToDeviceCopy[U](col, gpuCol, tupleNum, 0)

    this.globalSize = parent.globalSize
    this.localSize = parent.localSize
    gpuFilter = createReadWriteBuffer[Int](tupleNum)
    gpuPsum = createReadWriteBuffer[Int](globalSize)
    gpuCount = createReadWriteBuffer[Int](globalSize)

    val operationName = operation.toString

    val typeName = typeNameString[U]()

    val kernelName = "genScanFilter_init_%s_%s".format(typeName, operationName)

    var kernel = clCreateKernel(context.getOpenCLProgram, kernelName, null)

    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
    clSetKernelArg(kernel, 2, baseSize[U], pointer(toArray(value)))
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

    val resultSize: Int = tmp1(0) + tmp2(0)


    this.size = resultSize

    parent.byteData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Byte](index, this.size)
    })
    parent.shortData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Short](index, this.size)
    })
    parent.intData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Int](index, this.size)
    })
    parent.longData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Long](index, this.size)
    })
    parent.floatData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Float](index, this.size)
    })
    parent.doubleData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Double](index, this.size)
    })
    parent.booleanData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Boolean](index, this.size)
    })
    parent.charData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[Char](index, this.size)
    })
    parent.stringData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => project[String](index, this.size)
    })

    release  
  }

  def project[V: TypeTag](columnIndex: Int, outSize: Int) {
    println(f"outSize = $outSize, columnIndex = $columnIndex")
    if (outSize == 0)
      return
    val colData = parent.getColumn[V](columnIndex)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)
    val scanCol: cl_mem = createReadWriteBuffer[V](parent.size)
    assert(scanCol != null)
    assert(gpuPsum != null)

    hostToDeviceCopy[V](colData, scanCol, parent.size, 0)

    val result: cl_mem = createReadWriteBuffer[V](outSize)

    val colSize = baseSize[V]
    val kernelName: String = "scan_other"
    val kernel = clCreateKernel(context.getOpenCLProgram, kernelName, null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](colSize)))
    clSetKernelArg(kernel, 2, Sizeof.cl_long, Pointer.to(Array[Long](parent.size)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuPsum))
    clSetKernelArg(kernel, 4, Sizeof.cl_long, Pointer.to(Array[Long](this.size)))
    clSetKernelArg(kernel, 5, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 6, Sizeof.cl_mem, Pointer.to(result))

    clEnqueueNDRangeKernel(context.getOpenCLQueue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    val resultColData = this.getColumn[V](columnIndex)
    assert(resultColData != null)
    deviceToHostCopy[V](result, resultColData, outSize, 0)

    releaseCol(result)
    releaseCol(scanCol)
  }

}
