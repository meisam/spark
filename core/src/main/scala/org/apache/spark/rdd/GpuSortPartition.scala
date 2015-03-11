package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl.{Pointer, Sizeof}

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

class GpuSortPartition[T <: Product : TypeTag]
(context: OpenCLContext, parent: GpuPartition[T], sortColumnIndexes: Array[Int], sortDirections:
Array[SortDirection.Value], capacity: Int) extends GpuPartition[T](context, capacity) {

  def sort(): Unit = {
    /*

        struct timespec start,end;
        clock_gettime(CLOCK_REALTIME,&start);

        cl_event ndrEvt;
        cl_ulong startTime,endTime;

        struct tableNode * res = NULL;
        size_t globalSize, localSize;

        assert(odNode->table->tupleNum < SHARED_SIZE_LIMIT);

        res = (struct tableNode *)malloc(sizeof(struct tableNode));
        CHECK_POINTER(res);
        res->tupleNum = odNode->table->tupleNum;
        res->totalAttr = odNode->table->totalAttr;
        res->tupleSize = odNode->table->tupleSize;
        res->attrType = (int *) malloc(sizeof(int) * res->totalAttr);
        CHECK_POINTER(res->attrType);
        res->attrSize = (int *) malloc(sizeof(int) * res->totalAttr);
        CHECK_POINTER(res->attrSize);
        res->attrTotalSize = (int *) malloc(sizeof(int) * res->totalAttr);
        CHECK_POINTER(res->attrTotalSize);
        res->dataPos = (int *) malloc(sizeof(int) * res->totalAttr);
        CHECK_POINTER(res->dataPos);
        res->dataFormat = (int *) malloc(sizeof(int) * res->totalAttr);
        CHECK_POINTER(res->dataFormat);
        res->content = (char **) malloc(sizeof(char *) * res->totalAttr);
        CHECK_POINTER(res->content);

        int gpuTupleNum = odNode->table->tupleNum;
        cl_mem gpuKey, gpuContent;
        cl_mem gpuSortedKey;
        cl_mem gpuSize;
        cl_int error = 0;

        long totalSize = 0;
        long * cpuOffset = (long *)malloc(sizeof(long) * res->totalAttr);
        CHECK_POINTER(cpuOffset);
        long offset = 0;

        for(int i=0;i<res->totalAttr;i++){

            cpuOffset[i] = offset;
            res->attrType[i] = odNode->table->attrType[i];
            res->attrSize[i] = odNode->table->attrSize[i];
            res->attrTotalSize[i] = odNode->table->attrTotalSize[i];
            res->dataPos[i] = MEM;
            res->dataFormat[i] = UNCOMPRESSED;

            int size = res->attrSize[i] * res->tupleNum;

            if(size %4 !=0){
                size += (4 - size %4);
            }

            offset += size;
            totalSize += size;
        }

        gpuContent = clCreateBuffer(context->context,CL_MEM_READ_ONLY, totalSize, NULL, 0);
        for(int i=0;i<res->totalAttr;i++){

            int size = res->attrSize[i] * res->tupleNum;

            if(odNode->table->dataPos[i] == MEM){
                error = clEnqueueWriteBuffer(context->queue, gpuContent, CL_TRUE, cpuOffset[i], size, odNode->table->content[i],0,0,&ndrEvt);

            }else if (odNode->table->dataPos[i] == GPU){
                error = clEnqueueCopyBuffer(context->queue,(cl_mem)odNode->table->content[i],gpuContent,0,cpuOffset[i],size,0,0,0);
            }

        }

        cl_mem gpuOffset = clCreateBuffer(context->context,CL_MEM_READ_ONLY, sizeof(long)*res->totalAttr,NULL,0);
        error = clEnqueueWriteBuffer(context->queue, gpuOffset, CL_TRUE, 0, sizeof(long)*res->totalAttr, cpuOffset,0,0,&ndrEvt);

        int newNum = 1;

        while(newNum<gpuTupleNum){
            newNum *=2;
        }

        int dir;
        if(odNode->orderBySeq[0] == ASC)
            dir = 1;
        else
            dir = 0;

        int index = odNode->orderByIndex[0];
        int type = odNode->table->attrType[index];

        cl_mem gpuPos = clCreateBuffer(context->context, CL_MEM_READ_WRITE, sizeof(int)*newNum, NULL,0);
        gpuSize = clCreateBuffer(context->context,CL_MEM_READ_ONLY, res->totalAttr * sizeof(int), NULL, 0);
        clEnqueueWriteBuffer(context->queue, gpuSize, CL_TRUE, 0, sizeof(int)*res->totalAttr, res->attrSize,0,0,&ndrEvt);
        cl_mem gpuResult = clCreateBuffer(context->context,CL_MEM_READ_WRITE, totalSize, NULL,0);

        long * resOffset = (long *) malloc(sizeof(long) * res->totalAttr);
        CHECK_POINTER(resOffset);
        offset = 0;
        totalSize = 0;
        for(int i=0; i<res->totalAttr;i++){
            int size = res->attrSize[i] * res->tupleNum;
            if(size %4 != 0){
                size += 4 - (size % 4);
            }

            resOffset[i] = offset;
            offset += size;
            totalSize += size;
        }

        cl_mem gpuResOffset = clCreateBuffer(context->context,CL_MEM_READ_ONLY, sizeof(long)*res->totalAttr, NULL,0);
        clEnqueueWriteBuffer(context->queue, gpuResOffset, CL_TRUE, 0 ,sizeof(long)*res->totalAttr, resOffset, 0,0,&ndrEvt);

    if(type == INT){

        gpuKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int) * newNum, NULL, 0);
        gpuSortedKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int) * newNum, NULL, 0);

        localSize = 128;
        globalSize = 8*localSize;
        context->kernel = clCreateKernel(context->program,"set_key_int",0);
        error = clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void *)&gpuKey);
        error = clSetKernelArg(context->kernel,1,sizeof(int), (void *)&newNum);
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif

        clEnqueueWriteBuffer(context->queue, gpuKey, CL_TRUE, 0, sizeof(int)*gpuTupleNum, odNode->table->content[index],0,0,&ndrEvt);
        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->pcie += 1e-6 * (endTime - startTime);
        #endif

        context->kernel = clCreateKernel(context->program,"sort_key_int",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey);
        clSetKernelArg(context->kernel,1,sizeof(int), (void*)&newNum);
        clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&gpuSortedKey);
        clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&gpuPos);
        clSetKernelArg(context->kernel,4,sizeof(int), (void*)&dir);
        clSetKernelArg(context->kernel,5,SHARED_SIZE_LIMIT*sizeof(int), NULL);
        clSetKernelArg(context->kernel,6,SHARED_SIZE_LIMIT*sizeof(int), NULL);

        localSize = newNum/2;
        globalSize = localSize;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif

    }else if (type == FLOAT){

        gpuKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(float) * newNum, NULL, 0);
        gpuSortedKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(float) * newNum, NULL, 0);

        localSize = 128;
        globalSize = 8*localSize;
        context->kernel = clCreateKernel(context->program,"set_key_float",0);
        error = clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void *)&gpuKey);
        error = clSetKernelArg(context->kernel,1,sizeof(int), (void *)&newNum);
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif

        clEnqueueWriteBuffer(context->queue, gpuKey, CL_TRUE, 0, sizeof(float)*gpuTupleNum, odNode->table->content[index],0,0,&ndrEvt);
        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->pcie += 1e-6 * (endTime - startTime);
        #endif

        context->kernel = clCreateKernel(context->program,"sort_key_float",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey);
        clSetKernelArg(context->kernel,1,sizeof(int), (void*)&newNum);
        clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&gpuSortedKey);
        clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&gpuPos);
        clSetKernelArg(context->kernel,4,sizeof(int), (void*)&dir);
        clSetKernelArg(context->kernel,5,SHARED_SIZE_LIMIT*sizeof(float), NULL);
        clSetKernelArg(context->kernel,6,SHARED_SIZE_LIMIT*sizeof(int), NULL);

        localSize = newNum/2;
        globalSize = localSize;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif


    }else if (type == STRING){
        int keySize = odNode->table->attrSize[index];

        gpuKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, keySize * newNum, NULL, 0);
        gpuSortedKey = clCreateBuffer(context->context,CL_MEM_READ_WRITE, keySize * newNum, NULL, 0);

        localSize = 128;
        globalSize = 8*localSize;
        context->kernel = clCreateKernel(context->program,"set_key_string",0);
        int tmp = newNum * keySize;
        error = clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void *)&gpuKey);
        error = clSetKernelArg(context->kernel,1,sizeof(int), (void *)&tmp);
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif

        clEnqueueWriteBuffer(context->queue, gpuKey, CL_TRUE, 0, keySize*gpuTupleNum, odNode->table->content[index],0,0,&ndrEvt);
        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->pcie += 1e-6 * (endTime - startTime);
        #endif

        context->kernel = clCreateKernel(context->program,"sort_key_string",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey);
        clSetKernelArg(context->kernel,1,sizeof(int), (void*)&newNum);
        clSetKernelArg(context->kernel,2,sizeof(int), (void*)&keySize);
        clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&gpuSortedKey);
        clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&gpuPos);
        clSetKernelArg(context->kernel,5,sizeof(int), (void*)&dir);
        clSetKernelArg(context->kernel,6,SHARED_SIZE_LIMIT*keySize, NULL);
        clSetKernelArg(context->kernel,7,SHARED_SIZE_LIMIT*sizeof(int), NULL);

        localSize = newNum/2;
        globalSize = localSize;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif

    }

    if (odNode->orderByNum == 2){
        int keySize = odNode->table->attrSize[index];
        int secIndex = odNode->orderByIndex[1];
        int keySize2 = odNode->table->attrSize[secIndex];
        int secType = odNode->table->attrType[secIndex];
        cl_mem keyNum , keyCount, keyPsum;

        keyNum = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int), NULL, 0);

        if(type == INT){
            context->kernel = clCreateKernel(context->program,"count_unique_keys_int",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyNum);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif
        }else if (type == FLOAT){

            context->kernel = clCreateKernel(context->program,"count_unique_keys_float",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyNum);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif
        }else if (type == STRING){
            context->kernel = clCreateKernel(context->program,"count_unique_keys_string",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(int), (void*)&keySize);
            clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&keyNum);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif
        }
        int cpuKeyNum;
        clEnqueueReadBuffer(context->queue,keyNum, CL_TRUE, 0, sizeof(int), &cpuKeyNum,0,0,&ndrEvt);

        keyCount = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int)*cpuKeyNum, NULL,0);
        keyPsum = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int)*cpuKeyNum, NULL,0);

        if(type == INT){
            context->kernel = clCreateKernel(context->program,"count_key_num_int",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyCount);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif
        }else if (type == FLOAT){
            context->kernel = clCreateKernel(context->program,"count_key_num_float",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyCount);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

        }else if (type == STRING){
            context->kernel = clCreateKernel(context->program,"count_key_num_string",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuSortedKey);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,2,sizeof(int), (void*)&keySize);
            clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&keyCount);
            localSize = 1;
            globalSize = 1;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif
        }
    */

    this.size = parent.size
    inferBestWorkGroupSize()
    val gpuContent = createReadBuffer[Byte](this.columnOffsets.last)

    val memSetKernel = clCreateKernel(context.program, "cl_memset_char", null)
    val global_work_size = Array[Long](globalSize)
    val local_work_size = Array[Long](localSize)

    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpuContent))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](this.columnOffsets.last)))

    debugGpuBuffer[Byte](gpuContent, this.columnOffsets.last, "gpuContent before mem set")

    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)


    debugGpuBuffer[Byte](gpuContent, this.columnOffsets.last, "gpuContent after mem set")

    logInfo(f"Going to copy all columns (capcity = ${capacity}) to GPU with ")
    // copy all parent data to GPU
    columnTypes.zipWithIndex.foreach { case (colType, colIndex) =>
      val colSizeInBytes = baseSize(colType) * size
      val typedColIndex = toTypeAwareColumnIndex(colIndex)
      val offset = columnOffsets(colIndex)
      logInfo(f"column(${colIndex}) ${colType}(${typedColIndex}) size = ${colSizeInBytes}%,d bytes")
      logInfo(f"(${colIndex}) offset = ${offset}%,d out of ${this.columnOffsets.last}%,d")

      val column = parent.getColumn(typedColIndex)(colType)
      hostToDeviceCopy[Byte](column, gpuContent, colSizeInBytes, offset)
    }
    debugGpuBuffer[Byte](gpuContent, this.columnOffsets.last, "gpuContent after copying data")

    val gpuOffset = createReadBuffer[Long](columnTypes.length)
    hostToDeviceCopy[Long](Pointer.to(columnOffsets.map(_.toLong)), gpuOffset, columnTypes.length)
    debugGpuBuffer[Long](gpuOffset, columnTypes.length, "after copying offsets")

    val pow2s = (1 until 31).map(1 << _)

    val alignedPow2Size = pow2s.dropWhile(size > _).head
    logInfo(f"ceiling for ${size} is ${alignedPow2Size}")

    val sortDirection = sortDirections.head.id

    val sortColIndex = sortColumnIndexes.head

    val sortColTypes: Array[ru.TypeTag[_]] = sortColumnIndexes.map(i => columnTypes(i))

    val gpuPos = createReadWriteBuffer[Int](alignedPow2Size)
    val gpuSizes = createReadBuffer[Int](columnTypes.length)

    val colSizes = columnTypes.map(colType => baseSize(colType))
    hostToDeviceCopy[Int](Pointer.to(colSizes), gpuSizes, colSizes.length)

    debugGpuBuffer[Int](gpuSizes, colSizes.length, "gpuSizes after copying data")


    val gpuResult = createReadWriteBuffer[Byte](columnOffsets.last)

    def align(offset: Int): Long = {
      offset + (if (offset % 4 == 0) 0 else 4 - (offset % 4))
    }

    val resultOffsets = columnTypes.map(t => align(baseSize(t) * capacity)).scan(0L)(_ + _)

    logInfo( s"""column Offsets = ${columnOffsets.mkString(",")}""")
    logInfo( s"""aligned column offsets = ${resultOffsets.mkString(",")}""")

    val gpuResultsOffsets = createReadBuffer[Long](columnOffsets.length)
    hostToDeviceCopy[Long](Pointer.to(resultOffsets), gpuResultsOffsets, columnTypes.length)

    debugGpuBuffer[Long](gpuResultsOffsets, columnTypes.length, "gpu (aligned) Results Offsets")

    val sortColType = sortColTypes.head

    val gpuKey = createReadWriteBuffer(alignedPow2Size)(sortColType)
    val gpuSortedKey = createReadWriteBuffer(alignedPow2Size)(sortColType)

    clSetKernelArg(memSetKernel, 0, Sizeof.cl_mem, Pointer.to(gpuSortedKey))
    clSetKernelArg(memSetKernel, 1, Sizeof.cl_int, pointer(Array[Int](alignedPow2Size * baseSize(sortColType))))

    debugGpuBuffer[Byte](gpuSortedKey, alignedPow2Size * baseSize(sortColType), "gpuSortedKey before mem set")

    clEnqueueNDRangeKernel(context.queue, memSetKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)


    debugGpuBuffer[Byte](gpuSortedKey, alignedPow2Size * baseSize(sortColType), "gpuSortedKey after mem set")
    this.localSize = 128
    this.globalSize = 8 * localSize

    global_work_size(0) = globalSize
    local_work_size(0) = localSize

    val setKeyKernelName = s"set_key_${typeNameString()(sortColType)}"
    val setKeyKernel = clCreateKernel(context.program, setKeyKernelName, null)
    clSetKernelArg(setKeyKernel, 0, Sizeof.cl_mem, Pointer.to(gpuKey))
    val gpuKeyLength = if (sortColType.tpe =:= typeOf[String]) {
      baseSize[String] * alignedPow2Size // counting chars
    } else {
      alignedPow2Size
    }
    clSetKernelArg(setKeyKernel, 1, Sizeof.cl_int, Pointer.to(Array(gpuKeyLength)))
    clEnqueueNDRangeKernel(context.queue, setKeyKernel, 1, null, global_work_size,
      local_work_size, 0, null, null)

    val sortCol = parent.getColumn(sortColIndex)(sortColType)
    hostToDeviceCopy(sortCol, gpuKey, this.size, 0)(sortColType)
    debugGpuBuffer(gpuKey, alignedPow2Size, f"gpuKey copying keys")(sortColType)

    val SHARED_SIZE_LIMIT = 1024

    val sortKeyKernelName = s"sort_key_${typeNameString()(sortColType)}"
    val sortKeyKernel = clCreateKernel(context.program, sortKeyKernelName, null)
    clSetKernelArg(sortKeyKernel, 0, Sizeof.cl_mem, Pointer.to(gpuKey))
    clSetKernelArg(sortKeyKernel, 1, Sizeof.cl_int, Pointer.to(Array(alignedPow2Size)))
    clSetKernelArg(sortKeyKernel, 2, Sizeof.cl_mem, Pointer.to(gpuSortedKey))
    clSetKernelArg(sortKeyKernel, 3, Sizeof.cl_mem, Pointer.to(gpuPos))
    clSetKernelArg(sortKeyKernel, 4, Sizeof.cl_int, Pointer.to(Array(sortDirection)))
    clSetKernelArg(sortKeyKernel, 5, SHARED_SIZE_LIMIT * baseSize(sortColType), null)
    clSetKernelArg(sortKeyKernel, 6, SHARED_SIZE_LIMIT * Sizeof.cl_int, null)
    if (sortColType.tpe =:= typeOf[String]) {
      clSetKernelArg(sortKeyKernel, 7, Sizeof.cl_int, pointer(Array[Int](MAX_STRING_SIZE)))
    }

    localSize = alignedPow2Size / 2
    globalSize = localSize


    debugGpuBuffer(gpuSortedKey, alignedPow2Size, f"gpuSortedKey before ${sortKeyKernelName}")(sortColType)
    debugGpuBuffer[Int](gpuPos, alignedPow2Size, f"gpuPos before ${sortKeyKernelName}")
    debugGpuBuffer(gpuKey, alignedPow2Size, f"gpuKey before ${sortKeyKernelName}")(sortColType)

    clEnqueueNDRangeKernel(context.queue, sortKeyKernel, 1, null, Array[Long](globalSize),
      Array[Long](localSize), 0, null, null)

    debugGpuBuffer(gpuSortedKey, alignedPow2Size, f"gpuSortedKey after ${sortKeyKernelName}")(sortColType)
    debugGpuBuffer[Int](gpuPos, alignedPow2Size, f"gpuPos after ${sortKeyKernelName}")
    debugGpuBuffer(gpuKey, alignedPow2Size, f"gpuKey after ${sortKeyKernelName}")(sortColType)


    if (sortColumnIndexes.length == 2) {

      val secondSortColType = columnTypes(sortColumnIndexes(1))
      val keyNum = createReadWriteBuffer[Int](1)

      val countUniqueKeysKernelName = f"count_unique_keys_${typeNameString()(sortColType)}"
      val countUniqueKeysKernel = clCreateKernel(context.program, countUniqueKeysKernelName, null)

      clSetKernelArg(countUniqueKeysKernel,0,Sizeof.cl_mem, Pointer.to(gpuSortedKey))
      clSetKernelArg(countUniqueKeysKernel,1,Sizeof.cl_int, Pointer.to(Array[Int](parent.size)))
      clSetKernelArg(countUniqueKeysKernel,2,Sizeof.cl_mem, Pointer.to(keyNum))
      if (sortColType.tpe =:= typeOf[String]) {
        clSetKernelArg(countUniqueKeysKernel, 3, Sizeof.cl_int, pointer(Array[Int](MAX_STRING_SIZE)))
      }
      localSize = 1
      globalSize = 1
      clEnqueueNDRangeKernel(context.queue, countUniqueKeysKernel, 1, null, Array[Long](globalSize),
        Array[Long](localSize), 0, null, null)

      debugGpuBuffer[Int](keyNum, 1, f"keyNum after ${countUniqueKeysKernelName}")

      val cpuKeyNum = Array(-1)

      deviceToHostCopy[Int](keyNum, pointer(cpuKeyNum), 1)
      logInfo(f"cpuKeyNum = ${cpuKeyNum.head}")

      val keyCount = createReadWriteBuffer[Int](cpuKeyNum.head)

      val countKeyNumKernelName = f"count_key_num_${typeNameString()(sortColType)}"
      val countKeyNumKernel = clCreateKernel(context.program, countKeyNumKernelName, null)

      clSetKernelArg(countKeyNumKernel, 0, Sizeof.cl_mem, Pointer.to(gpuSortedKey))
      clSetKernelArg(countKeyNumKernel, 1, Sizeof.cl_int, Pointer.to(Array[Int](parent.size)))
      clSetKernelArg(countKeyNumKernel, 2, Sizeof.cl_mem, Pointer.to(keyCount))
      if (sortColType.tpe =:= typeOf[String]) {
        clSetKernelArg(countKeyNumKernel, 3, Sizeof.cl_int, pointer(Array[Int](MAX_STRING_SIZE)))
      }
      localSize = 1
      globalSize = 1
      clEnqueueNDRangeKernel(context.queue, countKeyNumKernel, 1, null, Array[Long](globalSize),
        Array[Long](localSize), 0, null, null)

      debugGpuBuffer[Int](keyCount, cpuKeyNum.head, f"keyCount after ${countKeyNumKernelName}")

      val keyPsum = createReadBuffer[Int](cpuKeyNum.head)
      scanImpl(keyCount, cpuKeyNum.head, keyPsum)

      val gpuPos2 = createReadWriteBuffer[Int](alignedPow2Size)
      val gpuKey2 = createReadBuffer(alignedPow2Size)(secondSortColType)
      val secondColumn = getColumn(sortColumnIndexes(1))(secondSortColType)

      val gatherColKernelName = f"gather_col_${typeNameString()(sortColType)}"
      val gatherColKernel = clCreateKernel(context.program, gatherColKernelName, null)
      clSetKernelArg(gatherColKernel, 0, Sizeof.cl_mem, Pointer.to(gpuPos))
      clSetKernelArg(gatherColKernel, 1, Sizeof.cl_mem, Pointer.to(secondColumn))
      clSetKernelArg(gatherColKernel, 2, Sizeof.cl_int, Pointer.to(Array(alignedPow2Size)))
      clSetKernelArg(gatherColKernel, 3, Sizeof.cl_int, Pointer.to(Array(parent.size)))
      clSetKernelArg(gatherColKernel, 4, Sizeof.cl_mem, Pointer.to(gpuKey2))
      clSetKernelArg(gatherColKernel, 5, Sizeof.cl_mem, Pointer.to(gpuPos2))
      localSize = 128
      globalSize = 8 * 128
      clEnqueueNDRangeKernel(context.queue, gatherColKernel, 1, null, Array[Long](globalSize),
        Array[Long](localSize), 0, null, null)

    } else {


      val gatherResultKernelName = "gather_result"
      val gatherResultKernel = clCreateKernel(context.program, "gather_result", null)
      clSetKernelArg(gatherResultKernel, 0, Sizeof.cl_mem, Pointer.to(gpuPos))
      clSetKernelArg(gatherResultKernel, 1, Sizeof.cl_mem, Pointer.to(gpuContent))
      clSetKernelArg(gatherResultKernel, 2, Sizeof.cl_int, Pointer.to(Array[Int](alignedPow2Size)))
      clSetKernelArg(gatherResultKernel, 3, Sizeof.cl_int, Pointer.to(Array[Int](parent.size)))
      clSetKernelArg(gatherResultKernel, 4, Sizeof.cl_mem, Pointer.to(gpuSizes))
      clSetKernelArg(gatherResultKernel, 5, Sizeof.cl_int, Pointer.to(Array(columnTypes.length)))
      clSetKernelArg(gatherResultKernel, 6, Sizeof.cl_mem, Pointer.to(gpuResult))
      clSetKernelArg(gatherResultKernel, 7, Sizeof.cl_mem, Pointer.to(gpuOffset))
      clSetKernelArg(gatherResultKernel, 8, Sizeof.cl_mem, Pointer.to(gpuResultsOffsets))

      localSize = 128;
      globalSize = 8 * localSize;
      clEnqueueNDRangeKernel(context.queue, gatherResultKernel, 1, null, Array[Long](globalSize),
        Array[Long](localSize), 0, null, null)
      debugGpuBuffer[Byte](gpuResult, columnOffsets.length, f"gpuResult after ${gatherResultKernelName}")
    }

    columnTypes.zipWithIndex.foreach { case (columnType, index) =>
      val column = getColumn(index)(columnType)
      deviceToHostCopy[Byte](gpuResult, column, parent.size * baseSize(columnType), resultOffsets(index).toInt)
    }

    /*

        scanImpl(keyCount, cpuKeyNum, keyPsum, context,pp);

        cl_mem gpuPos2, gpuKey2;
        gpuPos2 = clCreateBuffer(context->context,CL_MEM_READ_WRITE, sizeof(int)*newNum, NULL,0);
        gpuKey2 = clCreateBuffer(context->context,CL_MEM_READ_WRITE, keySize2*newNum, NULL,0);

        if(secType == INT){

            context->kernel = clCreateKernel(context->program,"gather_col_int",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&odNode->table->content[secIndex]);
            clSetKernelArg(context->kernel,2,sizeof(int), (void*)&newNum);
            clSetKernelArg(context->kernel,3,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&gpuKey2);
            localSize = 128;
            globalSize = 8*128;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

            context->kernel = clCreateKernel(context->program,"sec_sort_key_int",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey2);
            clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&keyPsum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyCount);
            clSetKernelArg(context->kernel,3,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuPos2);
            localSize = 1;
            globalSize = cpuKeyNum;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

        }else if (secType == FLOAT){

            context->kernel = clCreateKernel(context->program,"gather_col_float",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&odNode->table->content[secIndex]);
            clSetKernelArg(context->kernel,2,sizeof(int), (void*)&newNum);
            clSetKernelArg(context->kernel,3,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&gpuKey2);
            localSize = 128;
            globalSize = 8*128;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

            context->kernel = clCreateKernel(context->program,"sec_sort_key_float",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey2);
            clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&keyPsum);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyCount);
            clSetKernelArg(context->kernel,3,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,4,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuPos2);
            localSize = 1;
            globalSize = cpuKeyNum;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif


        }else if (secType == STRING){

            context->kernel = clCreateKernel(context->program,"gather_col_string",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,1,sizeof(cl_mem), (void*)&odNode->table->content[secIndex]);
            clSetKernelArg(context->kernel,2,sizeof(int), (void*)&newNum);
            clSetKernelArg(context->kernel,3,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,4,sizeof(int), (void*)&keySize2);
            clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuKey2);
            localSize = 128;
            globalSize = 8*128;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

            context->kernel = clCreateKernel(context->program,"sec_sort_key_string",0);
            clSetKernelArg(context->kernel,0,sizeof(cl_mem), (void*)&gpuKey2);
            clSetKernelArg(context->kernel,1,sizeof(int), (void*)&keySize);
            clSetKernelArg(context->kernel,2,sizeof(cl_mem), (void*)&keyPsum);
            clSetKernelArg(context->kernel,3,sizeof(cl_mem), (void*)&keyCount);
            clSetKernelArg(context->kernel,4,sizeof(int), (void*)&gpuTupleNum);
            clSetKernelArg(context->kernel,5,sizeof(cl_mem), (void*)&gpuPos);
            clSetKernelArg(context->kernel,6,sizeof(cl_mem), (void*)&gpuPos2);
            localSize = 1;
            globalSize = cpuKeyNum;
            error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,&ndrEvt);
            #ifdef OPENCL_PROFILE
            clWaitForEvents(1, &ndrEvt);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
            clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
            pp->kernel += 1e-6 * (endTime - startTime);
            #endif

        }

        context->kernel = clCreateKernel(context->program,"gather_result",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem),(void*)&gpuPos2);
        clSetKernelArg(context->kernel,1,sizeof(cl_mem),(void*)&gpuContent);
        clSetKernelArg(context->kernel,2,sizeof(int),(void*)&newNum);
        clSetKernelArg(context->kernel,3,sizeof(int),(void*)&gpuTupleNum);
        clSetKernelArg(context->kernel,4,sizeof(cl_mem),(void*)&gpuSize);
        clSetKernelArg(context->kernel,5,sizeof(int),(void*)&res->totalAttr);
        clSetKernelArg(context->kernel,6,sizeof(cl_mem),(void*)&gpuResult);
        clSetKernelArg(context->kernel,7,sizeof(cl_mem),(void*)&gpuOffset);
        clSetKernelArg(context->kernel,8,sizeof(cl_mem),(void*)&gpuResOffset);

        localSize = 128;
        globalSize = 8 * localSize;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,0);
        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif


        clReleaseMemObject(keyCount);
        clReleaseMemObject(keyNum);
        clReleaseMemObject(gpuPos2);
        clReleaseMemObject(gpuKey2);
    }else{

        context->kernel = clCreateKernel(context->program,"gather_result",0);
        clSetKernelArg(context->kernel,0,sizeof(cl_mem),(void*)&gpuPos);
        clSetKernelArg(context->kernel,1,sizeof(cl_mem),(void*)&gpuContent);
        clSetKernelArg(context->kernel,2,sizeof(int),(void*)&newNum);
        clSetKernelArg(context->kernel,3,sizeof(int),(void*)&gpuTupleNum);
        clSetKernelArg(context->kernel,4,sizeof(cl_mem),(void*)&gpuSize);
        clSetKernelArg(context->kernel,5,sizeof(int),(void*)&res->totalAttr);
        clSetKernelArg(context->kernel,6,sizeof(cl_mem),(void*)&gpuResult);
        clSetKernelArg(context->kernel,7,sizeof(cl_mem),(void*)&gpuOffset);
        clSetKernelArg(context->kernel,8,sizeof(cl_mem),(void*)&gpuResOffset);

        localSize = 128;
        globalSize = 8 * localSize;
        error = clEnqueueNDRangeKernel(context->queue, context->kernel, 1, 0, &globalSize,&localSize,0,0,0);

        #ifdef OPENCL_PROFILE
        clWaitForEvents(1, &ndrEvt);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_START,sizeof(cl_ulong),&startTime,0);
        clGetEventProfilingInfo(ndrEvt,CL_PROFILING_COMMAND_END,sizeof(cl_ulong),&endTime,0);
        pp->kernel += 1e-6 * (endTime - startTime);
        #endif
    }



    for(int i=0; i<res->totalAttr;i++){
        int size = res->attrSize[i] * gpuTupleNum;
        res->content[i] = (char *) malloc( size);
        CHECK_POINTER(res->content[i]);
        memset(res->content[i],0, size);
        clEnqueueReadBuffer(context->queue,gpuResult, CL_TRUE, resOffset[i], size, res->content[i],0,0,&ndrEvt);

    }

    free(resOffset);
    clFinish(context->queue);
    clReleaseMemObject(gpuKey);
    clReleaseMemObject(gpuContent);
    clReleaseMemObject(gpuResult);
    clReleaseMemObject(gpuSize);
    clReleaseMemObject(gpuPos);
    clReleaseMemObject(gpuOffset);
    clReleaseMemObject(gpuResOffset);

    clock_gettime(CLOCK_REALTIME,&end);
    double timeE = (end.tv_sec -  start.tv_sec)* BILLION + end.tv_nsec - start.tv_nsec;
    printf("orderBy Time: %lf\n", timeE/(1000*1000));

    return res;
 */
  }


  override def fill(itr: Iterator[T]): Unit = {
    super.fill(itr)
    this.sort()
  }
}

object SortDirection extends Enumeration {
  type OrderByDirection = Value
  val ASCENDING = Value(1, "Ascending")
  val DESCENDING = Value(0, "Descending")
}