package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof, cl_mem}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

class GpuFilteredPartition[T <: Product: TypeTag, U: TypeTag](context: OpenCLContext,
                                                              idx:Int, columnIndex: Int,
  operation: ComparisonOperation.Value, value: U, capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def filter(parent: GpuPartition[T]) = {

    if (parent.size == 0) {
      this.size = 0
    } else {

      val tupleNum = parent.size
      val gpuCol = if (typeOf[U] =:= ColumnarTypes.StringTypeTag.tpe) {
        createReadWriteBuffer[Byte](tupleNum * baseSize[U])
      } else {
        createReadWriteBuffer[U](tupleNum)
      }

      val col = parent.getColumn[U](columnIndex)
      hostToDeviceCopy[U](col, gpuCol, tupleNum, 0)

      this.globalSize = parent.globalSize
      this.localSize = parent.localSize
      val gpuFilter = createReadWriteBuffer[Int](tupleNum)
      val gpuPsum = createReadWriteBuffer[Int](globalSize)
      val gpuCount = createReadWriteBuffer[Int](globalSize)

      val operationName = operation.toString

      val typeName = typeNameString[U]()

      val kernelName = "genScanFilter_init_%s_%s".format(typeName, operationName)

      val genScanKernel = clCreateKernel(context.program, kernelName, null)

      clSetKernelArg(genScanKernel, 0, Sizeof.cl_mem, Pointer.to(gpuCol))
      clSetKernelArg(genScanKernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
      // string types are represented as char* in C/OpenCL and should be treated differently
      if (typeOf[U] =:= ColumnarTypes.StringTypeTag.tpe) {
        debugGpuBuffer[Char](gpuCol, tupleNum * MAX_STRING_SIZE, "gpuCol before mem set to zero")
        val conditionValueBuffer = new Array[Char](MAX_STRING_SIZE)
        val length = Math.min(value.toString().length(), MAX_STRING_SIZE)
        val stringBytes = value.toString().getChars(0, length, conditionValueBuffer, 0)

        val conditionValue = createReadBuffer[Char](MAX_STRING_SIZE)
        hostToDeviceCopy[Char](Pointer.to(conditionValueBuffer), conditionValue, MAX_STRING_SIZE)
        debugGpuBuffer[Byte](conditionValue, MAX_STRING_SIZE, "conditionValue")
        clSetKernelArg(genScanKernel, 2, Sizeof.cl_mem, Pointer.to(conditionValue))
      } else {
        clSetKernelArg(genScanKernel, 2, baseSize[U], pointer(toArray(value)))
      }
      clSetKernelArg(genScanKernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
      val global_work_size = Array[Long](globalSize)
      val local_work_size = Array[Long](localSize)
      clEnqueueNDRangeKernel(context.queue, genScanKernel, 1, null, global_work_size, local_work_size, 0, null, null)

      clReleaseMemObject(gpuCol)

      debugGpuBuffer[Int](gpuFilter, tupleNum, "gpuFilter after mem set to zero")

      val countScanNumKernel = clCreateKernel(context.program, "countScanNum", null)
      clSetKernelArg(countScanNumKernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
      clSetKernelArg(countScanNumKernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
      clSetKernelArg(countScanNumKernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
      clEnqueueNDRangeKernel(context.queue, countScanNumKernel, 1, null, global_work_size, local_work_size, 0, null, null)

      debugGpuBuffer[Int](gpuFilter, tupleNum, "gpuFilter after countScanNum")
      debugGpuBuffer[Int](gpuCount, globalSize, "gpuCount after countScanNum")
      scanImpl(gpuCount, globalSize, gpuPsum)

      val tmp1 = Array[Int](0)
      val tmp2 = Array[Int](0)

      deviceToHostCopy[Int](gpuCount, pointer(tmp1), 1, globalSize - 1)

      deviceToHostCopy[Int](gpuPsum, pointer(tmp2), 1, globalSize - 1)

      clReleaseMemObject(gpuCount)

      val resultSize: Int = tmp1(0) + tmp2(0)

      this.size = resultSize

      parent.byteData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Byte](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.shortData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Short](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.intData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Int](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.longData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Long](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.floatData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Float](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.doubleData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Double](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.booleanData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Boolean](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.charData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[Char](index, this.size, gpuFilter, gpuPsum, parent)
      })
      parent.stringData.zipWithIndex.filter(_._1 != null).foreach({
        case (_, index) => project[String](index, this.size, gpuFilter, gpuPsum, parent)
      })

      clReleaseMemObject(gpuFilter)
      clReleaseMemObject(gpuPsum)

      release
    }
  }

  def project[V: TypeTag](columnIndex: Int, outSize: Int, gpuFilter: cl_mem, gpuPsum: cl_mem, parent: GpuPartition[T]) {
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
    val kernel = clCreateKernel(context.program, kernelName, null)
    clSetKernelArg(kernel, 0, Sizeof.cl_mem, Pointer.to(scanCol))
    clSetKernelArg(kernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](colSize)))
    clSetKernelArg(kernel, 2, Sizeof.cl_long, Pointer.to(Array[Long](parent.size)))
    clSetKernelArg(kernel, 3, Sizeof.cl_mem, Pointer.to(gpuPsum))
    clSetKernelArg(kernel, 4, Sizeof.cl_long, Pointer.to(Array[Long](this.size)))
    clSetKernelArg(kernel, 5, Sizeof.cl_mem, Pointer.to(gpuFilter))
    clSetKernelArg(kernel, 6, Sizeof.cl_mem, Pointer.to(result))

    clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    val resultColData = this.getColumn[V](columnIndex)
    assert(resultColData != null)
    deviceToHostCopy[V](result, resultColData, outSize, 0)

    releaseCol(result)
    releaseCol(scanCol)
  }

}
