
__kernel void genScanFilter_init_int_gth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] > where;
                filter[i] = con;
        }
}

__kernel void genScanFilter_init_int_lth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] < where;
                filter[i] = con;
        }
}

__kernel void genScanFilter_init_int_geq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] >= where;
                filter[i] = con;
        }
}

__kernel void genScanFilter_init_int_leq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] <= where;
                filter[i] = con;
        }
}

__kernel void genScanFilter_init_int_eq(__global int *col, long tupleNum, int where, __global int * filter){
        size_t stride = get_global_size(0);
        size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] == where;
                filter[i] = con;
        }
}



__kernel void genScanFilter_and_int_eq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] == where;
                filter[i] &= con;
        }
}

__kernel void genScanFilter_and_int_geq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] >= where;
                filter[i] &= con;
        }
}


__kernel void genScanFilter_and_int_leq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] <= where;
                filter[i] &= con;
        }
}


__kernel void genScanFilter_and_int_gth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] > where;
                filter[i] &= con;
        }
}


__kernel void genScanFilter_and_int_lth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] < where;
                filter[i] &= con;
        }
}

__kernel void genScanFilter_or_int_eq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] == where;
                filter[i] |= con;
        }
}

__kernel void genScanFilter_or_int_gth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] > where;
                filter[i] |= con;
        }
}

__kernel void genScanFilter_or_int_lth(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] < where;
                filter[i] |= con;
        }
}

__kernel void genScanFilter_or_int_geq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] >= where;
                filter[i] |= con;
        }
}

__kernel void genScanFilter_or_int_leq(__global int *col, long tupleNum, int where, __global int * filter){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int con;

        for(size_t i = tid; i<tupleNum;i+=stride){
                con = col[i] <= where;
                filter[i] |= con;
        }
}

__kernel void countScanNum(__global int *filter, long tupleNum, __global int * count){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int localCount = 0;

        for(size_t i = tid; i<tupleNum; i += stride){
                localCount += filter[i];
        }

        count[tid] = localCount;

}


__kernel void scan_other(__global char *col, int colSize, long tupleNum, __global int *psum, long resultNum, __global int * filter, __global char * result){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int pos = psum[tid]  * colSize;

        for(size_t i = tid; i<tupleNum;i+=stride){

                if(filter[i] == 1){
            for(int k=0;k<colSize;k++)
                (result+pos)[k] = (col+i*colSize)[k];
                        pos += colSize;
                }
        }
}

__kernel void scan_int(__global int *col, int colSize, long tupleNum, __global int *psum, long resultNum, __global int * filter, __global int * result){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
        int localCount = psum[tid] ;

        for(size_t i = tid; i<tupleNum;i+=stride){

                if(filter[i] == 1){
                        result[localCount] = col[i];
                        localCount ++;
                }
        }
}


// for atomic add on float type data

inline void AtomicAdd(__global float *source, float operand) {
    union {
        unsigned int intVal;
        float floatVal;
    } newVal;
    union {
        unsigned int intVal;
        float floatVal;
    } prevVal;
    do {
        prevVal.floatVal = *source;
        newVal.floatVal = prevVal.floatVal + operand;
    } while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
}

// for prefixsum 
#define NUM_BANKS 16
#define LOG_NUM_BANKS 4

inline int CONFLICT_FREE_OFFSET(int index)
{
        return ((index) >> LOG_NUM_BANKS);
}

inline void loadSharedChunkFromMem(__local int *s_data,
                                                                           __global int *g_idata,
                                                                           int n, int baseIndex,
                                                                           int* ai, int* bi,
                                                                           int* mem_ai, int* mem_bi,
                                                                           int* bankOffsetA, int* bankOffsetB, int isNP2)
{
        size_t thid = get_local_id(0);
        *mem_ai = baseIndex + thid;
        *mem_bi = *mem_ai + get_local_size(0);

        *ai = thid;
        *bi = thid + get_local_size(0);

        // compute spacing to avoid bank conflicts
        *bankOffsetA = CONFLICT_FREE_OFFSET(*ai);
        *bankOffsetB = CONFLICT_FREE_OFFSET(*bi);

        s_data[*ai + *bankOffsetA] = g_idata[*mem_ai];

        if (isNP2)
        {
                s_data[*bi + *bankOffsetB] = (*bi < n) ? g_idata[*mem_bi] : 0;
        }
        else
        {
                s_data[*bi + *bankOffsetB] = g_idata[*mem_bi];
        }
}

inline void storeSharedChunkToMem(__global int* g_odata,
                                      __local int* s_data,
                                      int n,
                                      int ai, int bi,
                                      int mem_ai, int mem_bi,
                                      int bankOffsetA, int bankOffsetB, int isNP2)
{
    barrier(CLK_LOCAL_MEM_FENCE); 

    g_odata[mem_ai] = s_data[ai + bankOffsetA];
    if (isNP2)
    {
        if (bi < n)
            g_odata[mem_bi] = s_data[bi + bankOffsetB];
    }
    else
    {
        g_odata[mem_bi] = s_data[bi + bankOffsetB];
    }
}

inline void clearLastElement(__local int* s_data,
                                 __global int *g_blockSums,
                                 int blockIndex, int storeSum)
{
    if (get_local_id(0) == 0)
    {
        int index = (get_local_size(0) << 1) - 1;
        index += CONFLICT_FREE_OFFSET(index);

        if (storeSum) 
        {
            g_blockSums[blockIndex] = s_data[index];
        }

        s_data[index] = 0;
    }
}

inline int buildSum(__local int *s_data)
{
    int thid = get_local_id(0);
    int stride = 1;

    for (size_t d = get_local_size(0); d > 0; d >>= 1)
    {
    barrier(CLK_LOCAL_MEM_FENCE);

        if (thid < d)
        {
            int i  = mul24(mul24(2, stride), thid);
            int ai = i + stride - 1;
            int bi = ai + stride;

            ai += CONFLICT_FREE_OFFSET(ai);
            bi += CONFLICT_FREE_OFFSET(bi);

            s_data[bi] += s_data[ai];
        }

        stride *= 2;
    }

    return stride;
}

void scanRootToLeaves(__local int *s_data, int stride)
{
    int thid = get_local_id(0);

    for (size_t d = 1; d <= get_local_size(0); d *= 2)
    {
        stride >>= 1;

    barrier(CLK_LOCAL_MEM_FENCE);

        if (thid < d)
        {
            int i  = mul24(mul24(2, stride), thid);
            int ai = i + stride - 1;
            int bi = ai + stride;

            ai += CONFLICT_FREE_OFFSET(ai);
            bi += CONFLICT_FREE_OFFSET(bi);

            int t  = s_data[ai];
            s_data[ai] = s_data[bi];
            s_data[bi] += t;
        }
    }
}

void prescanBlock(__local int *data, int blockIndex, __global int *blockSums, int storeSum)
{
    int stride = buildSum(data);               // build the sum in place up the tree
    clearLastElement(data, blockSums,
                               (blockIndex == 0) ? get_group_id(0) : blockIndex, storeSum);
    scanRootToLeaves(data, stride);            // traverse down tree to build the scan 
}

__kernel void prescan(__global int *g_odata,
                        __global int *g_idata,
                        __global int *g_blockSums,
                        int n,
                        int blockIndex,
                        int baseIndex, int storeSum, int isNP2, int same, __local int * s_data
                                                )
{
    int ai, bi, mem_ai, mem_bi, bankOffsetA, bankOffsetB;
    int bid = get_group_id(0);
    int bsize = get_local_size(0);

    loadSharedChunkFromMem(s_data, (same == 0) ? g_idata:g_odata,
                  n,
                                  (baseIndex == 0) ?
                                  mul24(bid, (bsize << 1)):baseIndex,
                                  &ai, &bi, &mem_ai, &mem_bi,
                                  &bankOffsetA, &bankOffsetB, isNP2);

    prescanBlock(s_data, blockIndex, g_blockSums,storeSum);

    storeSharedChunkToMem(g_odata, s_data, n,
                                 ai, bi, mem_ai, mem_bi,
                                 bankOffsetA, bankOffsetB, isNP2);
}



__kernel void uniformAdd(__global int *g_data,
                           __global int *uniforms,
                           int n,
                           int blockOffset,
                           int baseIndex)
{
    __local int uni;
    if (get_local_id(0) == 0)
        uni = uniforms[get_group_id(0) + blockOffset];

    int bid = get_group_id(0);
    int bsize = get_local_size(0);

    int address = mul24(bid, (bsize << 1)) + baseIndex + get_local_id(0);

    barrier(CLK_LOCAL_MEM_FENCE);

    g_data[address]              += uni;
    g_data[address + get_local_size(0)] += (get_local_id(0) + get_local_size(0) < n) * uni;
}

__kernel void prefix_sum_stage(__global int *prefix_sum_in,
                           __global int *prefix_sum_out,
                           const int stride)
{
    int thread_idx = get_global_id(0);
    if (stride == 0) {
        prefix_sum_out[thread_idx] = prefix_sum_in[thread_idx];
        return;
    }
    unsigned long my_sum = prefix_sum_in[thread_idx];
    unsigned long neighbors_sum = (thread_idx < stride)? 0 :prefix_sum_in[thread_idx - stride];
    prefix_sum_out[thread_idx] = my_sum + neighbors_sum;
}

__kernel void scan(__global int *source_col,
                        __global int *filter,
                        __global int *prefix_sum,
                        __global int *dest_col,
                        const int n)
{
    int thread_idx = get_global_id(0);
    if ((thread_idx < n) && (filter[thread_idx] == 1)) {
        int my_write_index = prefix_sum[thread_idx] - 1;
        dest_col[my_write_index] = source_col[thread_idx];
    }
}
