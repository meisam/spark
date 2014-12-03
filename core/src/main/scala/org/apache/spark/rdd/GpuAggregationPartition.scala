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
  groupByColumnIndexes: Array[Int], aggregations: Array[AggregationOperation.Value], capacity: Int)
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

    println("cpuOffsets = %s".format(cpuOffsets.mkString(",")))

    println(f"totalSize=$totalSize")

    val gpuContent = createReadBuffer[Byte](totalSize) // [Byte] because everything is in bytes

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
    printf("gpuContentResults = %s\n", gpuContentResults.mkString(","))

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
    // @gpuGbExp is the mathExp in each groupBy expression
    // @mathexp stores the math exp for for the group expression that has two operands
    // The reason that we need two variables instead of one is that OpenCL doesn't support pointer to pointer
    //

    /*

    cl_mem gpuGbExp = clCreateBuffer(context->context, CL_MEM_READ_ONLY, sizeof(struct mathExp)*res->totalAttr, NULL, &error);
    cl_mem mathexp = clCreateBuffer(context->context, CL_MEM_READ_ONLY, 2*sizeof(struct mathExp)*res->totalAttr, NULL, &error);

    struct mathExp tmpExp[2];
    int * cpuFunc = (int *) malloc(sizeof(int) * res->totalAttr);
    CHECK_POINTER(cpuFunc);

    offset = 0;
    for(int i=0;i<res->totalAttr;i++){

        error = clEnqueueWriteBuffer(context->queue, gpuGbExp, CL_TRUE, offset, sizeof(struct mathExp), &(gb->gbExp[i].exp),0,0,&ndrEvt);

#ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->pcie += 1e-6 * (endTime - startTime);
#endif

        offset += sizeof(struct mathExp);

        cpuFunc[i] = gb->gbExp[i].func;

        if(gb->gbExp[i].exp.opNum == 2){
            struct mathExp * tmpMath = (struct mathExp *) (gb->gbExp[i].exp.exp);
            tmpExp[0].op = tmpMath[0].op;
            tmpExp[0].opNum = tmpMath[0].opNum;
            tmpExp[0].opType = tmpMath[0].opType;
            tmpExp[0].opValue = tmpMath[0].opValue;

            tmpExp[1].op = tmpMath[1].op;
            tmpExp[1].opNum = tmpMath[1].opNum;
            tmpExp[1].opType = tmpMath[1].opType;
            tmpExp[1].opValue = tmpMath[1].opValue;
            clEnqueueWriteBuffer(context->queue, mathexp, CL_TRUE, 2*i*sizeof(struct mathExp),2*sizeof(struct mathExp),tmpExp,0,0,&ndrEvt);

#ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->pcie += 1e-6 * (endTime - startTime);
#endif
        }
    }

    cl_mem gpuFunc = clCreateBuffer(context->context, CL_MEM_READ_ONLY, sizeof(int)*res->totalAttr, NULL, &error);
    clEnqueueWriteBuffer(context->queue,gpuFunc,CL_TRUE,0,sizeof(int)*res->totalAttr,cpuFunc,0,0,&ndrEvt);

#ifdef OPENCL_PROFILE
    clWaitForEvents(1, &ndrEvt);
    clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
    clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
    pp->pcie += 1e-6 * (endTime - startTime);
#endif

    long *resOffset = (long *)malloc(sizeof(long)*res->totalAttr);
    CHECK_POINTER(resOffset);
    
    offset = 0;
    totalSize = 0;
    for(int i=0;i<res->totalAttr;i++){
        
        /*
         * align the output of each column on the boundary of 4
         */

        int size = res->attrSize[i] * res->tupleNum;
        if(size %4 != 0){
            size += 4- (size %4);
        }

        resOffset[i] = offset;
        offset += size; 
        totalSize += size;
    }

    cl_mem gpuResult = clCreateBuffer(context->context,CL_MEM_READ_WRITE, totalSize, NULL, &error);
    cl_mem gpuResOffset = clCreateBuffer(context->context, CL_MEM_READ_ONLY,sizeof(long)*res->totalAttr, NULL,&error);
    clEnqueueWriteBuffer(context->queue,gpuResOffset,CL_TRUE,0,sizeof(long)*res->totalAttr,resOffset,0,0,&ndrEvt);

#ifdef OPENCL_PROFILE
    clWaitForEvents(1, &ndrEvt);
    clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
    clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
    pp->pcie += 1e-6 * (endTime - startTime);
#endif

    gpuGbColNum = res->totalAttr;

    if(gbConstant !=1){
        context->kernel = clCreateKernel(context->program,"agg_cal",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuContent);
        clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&gpuOffset);
        clSetKernelArg(context->kernel,2,sizeof(int), (void*)&gpuGbColNum);
        clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&gpuGbExp);
        clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&mathexp);
        clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuGbType);
        clSetKernelArg(context->kernel,6,sizeof(cl_mem), (void*)&gpuGbSize);
        clSetKernelArg(context->kernel,7,sizeof(long), (void*)&gpuTupleNum);
        clSetKernelArg(context->kernel,8,sizeof(cl_mem), (void*)&gpuGbKey);
        clSetKernelArg(context->kernel,9,sizeof(cl_mem), (void*)&gpu_psum);
        clSetKernelArg(context->kernel,10,sizeof(cl_mem), (void*)&gpuResult);
        clSetKernelArg(context->kernel,11,sizeof(cl_mem), (void*)&gpuResOffset);
        clSetKernelArg(context->kernel,12,sizeof(cl_mem), (void*)&gpuFunc);

        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
#ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
#endif
        
        clReleaseMemObject(gpuGbKey);
        clReleaseMemObject(gpu_psum);
    }else{
        context->kernel = clCreateKernel(context->program,"agg_cal_cons",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuContent);
        clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&gpuOffset);
        clSetKernelArg(context->kernel,2,sizeof(int), (void*)&gpuGbColNum);
        clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&gpuGbExp);
        clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&mathexp);
        clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuGbType);
        clSetKernelArg(context->kernel,6,sizeof(cl_mem), (void*)&gpuGbSize);
        clSetKernelArg(context->kernel,7,sizeof(long), (void*)&gpuTupleNum);
        clSetKernelArg(context->kernel,8,sizeof(cl_mem), (void*)&gpuResult);
        clSetKernelArg(context->kernel,9,sizeof(cl_mem), (void*)&gpuResOffset);
        clSetKernelArg(context->kernel,10,sizeof(cl_mem), (void*)&gpuFunc);

        globalSize = localSize * 4;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
#ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
#endif
    }

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

    clock_gettime(CLOCK_REALTIME,&end);
    double timeE = (end.tv_sec -  start.tv_sec)* BILLION + end.tv_nsec - start.tv_nsec;
    printf("GroupBy Time: %lf\n", timeE/(1000*1000));

    return res;
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

  def count = Value("count")

  def min = Value("min")

  def max = Value("max")

  def sum = Value("sum")

  def avg = Value("avg")
}

class MathExp(op: Int, opNum: Int, exp: Long, opType: Int, opValue: Int) {

  def writeTo(out: ByteBuffer): Unit = {
    out.order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(op)
    out.putInt(opNum)
    out.putLong(exp)
    out.putInt(opType)
    out.putInt(opValue)
  }

  def size = 4 * (1 + 1 + 2 + 1 + 1) // 4 bytes per word *( Words in Int, Int, Long, Int, Int)
}