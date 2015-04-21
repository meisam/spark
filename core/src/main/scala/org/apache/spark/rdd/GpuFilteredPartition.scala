package org.apache.spark.rdd

import java.nio.{ByteBuffer, CharBuffer, Buffer}

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof, cl_mem}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

class GpuFilteredPartition[T <: Product: TypeTag, U: TypeTag](context: OpenCLContext,
                                                              idx:Int, columnIndex: Int,
  operation: ComparisonOperation.Value, value: U, capacity: Int)
  extends GpuPartition[T](context, capacity) {

  def filter(parent: GpuPartition[T]): Int = {
    val startFilterTime = System.nanoTime()

    if (parent.size == 0) {
      this.size = 0
      this.size
    } else {

      val tupleNum = parent.size
      val gpuCol = if (isStringType[U]) {
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
      if (isStringType[U]) {
        debugGpuBuffer[String](gpuCol, tupleNum, "gpuCol before mem set to zero", false)
        val conditionValueBuffer = new Array[Byte](MAX_STRING_SIZE)
        val length = Math.min(value.toString().length(), MAX_STRING_SIZE)
        val stringBytes = value.toString().getBytes(0, length, conditionValueBuffer, 0)

        val conditionValue = createReadBuffer[Byte](MAX_STRING_SIZE)
        hostToDeviceCopy[Byte](Pointer.to(conditionValueBuffer), conditionValue, MAX_STRING_SIZE)
        debugGpuBuffer[String](conditionValue, 1, "conditionValue", false)
        clSetKernelArg(genScanKernel, 2, Sizeof.cl_mem, Pointer.to(conditionValue))
      } else {
        clSetKernelArg(genScanKernel, 2, baseSize[U], pointer(toArray(value)))
      }
      clSetKernelArg(genScanKernel, 3, Sizeof.cl_mem, Pointer.to(gpuFilter))
      val global_work_size = Array[Long](globalSize)
      val local_work_size = Array[Long](localSize)
      clEnqueueNDRangeKernel(context.queue, genScanKernel, 1, null, global_work_size, local_work_size, 0, null, null)

      clReleaseMemObject(gpuCol)

      debugGpuBuffer[Int](gpuFilter, tupleNum, "gpuFilter after genScanFilter_init_")

      val countScanNumKernel = clCreateKernel(context.program, "countScanNum", null)
      clSetKernelArg(countScanNumKernel, 0, Sizeof.cl_mem, Pointer.to(gpuFilter))
      clSetKernelArg(countScanNumKernel, 1, Sizeof.cl_long, Pointer.to(Array[Long](tupleNum)))
      clSetKernelArg(countScanNumKernel, 2, Sizeof.cl_mem, Pointer.to(gpuCount))
      clEnqueueNDRangeKernel(context.queue, countScanNumKernel, 1, null, global_work_size, local_work_size, 0, null, null)

      debugGpuBuffer[Int](gpuFilter, tupleNum, "gpuFilter after countScanNum", false)
      debugGpuBuffer[Int](gpuCount, globalSize, "gpuCount after countScanNum", false)
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

      val endFilterTime = System.nanoTime()

      logInfo(f"filter time = ${endFilterTime - startFilterTime}%,d")

      clReleaseMemObject(gpuFilter)
      clReleaseMemObject(gpuPsum)

      release
      this.size
    }
  }

  def project[V: TypeTag](columnIndex: Int, outSize: Int, gpuFilter: cl_mem, gpuPsum: cl_mem, parent: GpuPartition[T]) {
    val startTime = System.nanoTime()
    if (outSize == 0)
      return
    val colData = parent.getColumn[V](columnIndex, true)
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

    debugGpuBuffer[V](result, outSize, "result before scan_other", false)
    logInfo(f"globalSize = $globalSize, localSize=$localSize")

    clEnqueueNDRangeKernel(context.queue, kernel, 1, null, global_work_size, local_work_size, 0, null, null)

    debugGpuBuffer[V](result, outSize, "result after scan_other", false)

    val resultColData: Buffer = this.getColumn[V](columnIndex, true)
    assert(resultColData != null)
    deviceToHostCopy[V](result, resultColData, outSize, 0)

    val identity =     System.identityHashCode(resultColData)
    logInfo(f"resultColData.class = ${resultColData.getClass} with identity=$identity")
    if (resultColData.isInstanceOf[ByteBuffer]) {
      val byteBuffer = resultColData.asInstanceOf[ByteBuffer]
      logInfo(f"column($columnIndex) is byte buffer = ${byteBuffer.toString}")

      val start = byteBuffer.position()

      val end = byteBuffer.limit()
      val bytes = new Array[Byte](math.min(end -start, 500))

      byteBuffer.get(bytes)

      val str = new String(bytes)

      logInfo(f"resultColData = $str")

    }

    releaseCol(result)
    releaseCol(scanCol)
    val endTime = System.nanoTime()
    logInfo(f"projection time = ${endTime - startTime}%,d")
  }

}
