typedef unsigned char boolean;

#define HSIZE 131072
#define MAX_SRING_SIZE (1 << 7)

// The order of these definitions should be the same as the order in the counterpart scala files
// The scala definitions are in GpuPartiotion

enum data_types {
    BYTE = 0,
    SHORT = 1,
    INT = 2,
    LONG = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BOOLEAN = 6,
    CHAR = 7,
    STRING = 8
};

enum aggregation_operations {
    GROUPBY = 0, //
    MIN = 1, //
    MAX = 2, //
    COUNT = 3, //
    SUM = 4, //
    AVG = 5
};

enum math_exp_operand_type {
    COLUMN = 0, //
    CONS = 1
};

enum math_operations {
    NOOP = 0, //
    PLUS = 1, //  
    MINUS = 2, //
    MULTIPLY = 3, //
    DIVIDE = 4 //
};

struct mathExp {
    int op; /* the math operation */
    int opNum; /* the number of operands */

    long exp; /* if the opNum is 2, this field stores pointer that points to the two operands whose type is mathExp */

    /* when opNum is 1 */
    int opType; /* whether it is a regular column or a constant */
    int opValue; /* it is the index of the column or the value of the constant */
};

#define genScanFilter(assign_name,assign_operation,column_type, operation_name, operation)   \
__kernel void genScanFilter_##assign_name##_##column_type##_##operation_name                 \
(__global column_type *col, long tupleNum, column_type where, __global int * filter)         \
{                                                                                            \
    size_t stride = get_global_size(0);                                                      \
    size_t tid = get_global_id(0);                                                           \
        int con;                                                                             \
                                                                                             \
        for(size_t i = tid; i<tupleNum;i+=stride){                                           \
                con = col[i] operation where;                                                \
                filter[i] assign_operation con;                                              \
        }                                                                                    \
}                                                                                            \
                                                                                             \

#define genScanFilter_string(assign_name,assign_operation, operation_name, operation)        \
__kernel void genScanFilter_##assign_name##_string_##operation_name                          \
(__global char *col, long tupleNum, __global char *where, __global int * filter)             \
{                                                                                            \
    size_t stride = get_global_size(0);                                                      \
    size_t tid = get_global_id(0);                                                           \
        int con = 1;                                                                         \
                                                                                             \
        for(size_t i = tid; i<tupleNum;i+=stride){                                           \
                for(int k = 0; k < MAX_SRING_SIZE; k++) {                                    \
                    con = con && col[i * MAX_SRING_SIZE + k] operation where[k];             \
                }                                                                            \
                filter[i] assign_operation con;                                              \
        }                                                                                    \
}                                                                                            \
                                                                                             \

#define declare_genScanFilter(column_type, operation_name, operation)                        \
genScanFilter(init, =, column_type, operation_name, operation)                               \
genScanFilter(and, &=, column_type, operation_name, operation)                               \
genScanFilter(or, |=, column_type, operation_name, operation)                                \

#define define_gen_scan_kernels(column_type)                         \
declare_genScanFilter(column_type, lth, < )                          \
declare_genScanFilter(column_type, leq, <=)                          \
declare_genScanFilter(column_type, gth, > )                          \
declare_genScanFilter(column_type, geq, >=)                          \
declare_genScanFilter(column_type, eql, ==)                          \
declare_genScanFilter(column_type, neq, !=)                          \

#define declare_genScan_string_Filter(operation_name, operation)                       \
genScanFilter_string(init, =, operation_name, operation)                               \
genScanFilter_string(and, &=, operation_name, operation)                               \
genScanFilter_string(or, |=, operation_name, operation)                                \

#define define_gen_scan_string_kernels                          \
declare_genScan_string_Filter(lth, < )                          \
declare_genScan_string_Filter(leq, <=)                          \
declare_genScan_string_Filter(gth, > )                          \
declare_genScan_string_Filter(geq, >=)                          \
declare_genScan_string_Filter(eql, ==)                          \
declare_genScan_string_Filter(neq, !=)                          \

define_gen_scan_kernels(int)
define_gen_scan_kernels(long)
define_gen_scan_kernels(float)
define_gen_scan_kernels(double)
define_gen_scan_kernels(boolean)
define_gen_scan_kernels(char)
// strings need special treatment
define_gen_scan_string_kernels

// Sets all the values on the given buffer to zero
#define declare_cl_memset(buffer_type)                                      \
__kernel void cl_memset_##buffer_type(__global buffer_type * ar, int num){  \
        size_t stride = get_global_size(0);                                 \
        size_t offset = get_global_id(0);                                   \
                                                                            \
        for(size_t i=offset; i<num; i+= stride)                             \
                ar[i] = 0;                                                  \
}                                                                           \

declare_cl_memset(int)
declare_cl_memset(long)
declare_cl_memset(float)
declare_cl_memset(double)
declare_cl_memset(boolean)
declare_cl_memset(char)

#define IEEE_NAN (0x7fffffff)

__kernel void cl_memset_nan(__global int * ar, int num, int offset) {       \
        size_t stride = get_global_size(0);                                 \
        size_t start = get_global_id(0) + offset;                           \
                                                                            \
        for(size_t i=start; i<num+offset; i+= stride)                       \
                ar[i] = IEEE_NAN;                                           \
}                                                                           \

__kernel void countScanNum(__global int *filter, long tupleNum, __global int * count) {
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
    int localCount = 0;

    for(size_t i = tid; i<tupleNum; i += stride) {
        localCount += filter[i];
    }

    count[tid] = localCount;

}

__kernel void scan_other(__global char *col, int colSize, long tupleNum, __global int *psum, long resultNum, __global int * filter, __global char * result) {
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
    int pos = psum[tid] * colSize;

    for(size_t i = tid; i<tupleNum;i+=stride) {

        if(filter[i] == 1) {
            for(int k=0;k<colSize;k++)
            (result+pos)[k] = (col+i*colSize)[k];
            pos += colSize;
        }
    }
}

__kernel void scan_int(__global int *col, int colSize, long tupleNum, __global int *psum, long resultNum, __global int * filter, __global int * result) {
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
    int localCount = psum[tid];

    for(size_t i = tid; i<tupleNum;i+=stride) {

        if(filter[i] == 1) {
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
    }newVal;
    union {
        unsigned int intVal;
        float floatVal;
    }prevVal;
    do {
        prevVal.floatVal = *source;
        newVal.floatVal = prevVal.floatVal + operand;
    }while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
}

// for atomic max on float type data
inline void AtomicMin(__global float *source, float operand) {
    union {
        unsigned int intVal;
        float floatVal;
    }newVal;
    union {
        unsigned int intVal;
        float floatVal;
    }prevVal;
    do {
        prevVal.floatVal = *source;
        if (prevVal.intVal != IEEE_NAN) {
            if (operand > prevVal.floatVal) {
                return;
            }
        }
        newVal.floatVal = operand;
    }while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
}

// for atomic min on float type data
inline void AtomicMax(__global float *source, float operand) {
    union {
        unsigned int intVal;
        float floatVal;
    }newVal;
    union {
        unsigned int intVal;
        float floatVal;
    }prevVal;
    do {
        prevVal.floatVal = *source;
        if (prevVal.intVal != IEEE_NAN) {
            if (operand < prevVal.floatVal) {
                return;
            }
        }
        newVal.floatVal = operand;
    }while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
}

// My GPU does not support atomic operations for long and double data. How to implement them?

// for prefixsum 
#define NUM_BANKS 16
#define LOG_NUM_BANKS 4

inline int CONFLICT_FREE_OFFSET(int index) {
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
            int i = mul24(mul24(2, stride), thid);
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
            int i = mul24(mul24(2, stride), thid);
            int ai = i + stride - 1;
            int bi = ai + stride;

            ai += CONFLICT_FREE_OFFSET(ai);
            bi += CONFLICT_FREE_OFFSET(bi);

            int t = s_data[ai];
            s_data[ai] = s_data[bi];
            s_data[bi] += t;
        }
    }
}

void prescanBlock(__local int *data, int blockIndex, __global int *blockSums, int storeSum)
{
    int stride = buildSum(data);           // build the sum in place up the tree
    clearLastElement(data, blockSums,
            (blockIndex == 0) ? get_group_id(0) : blockIndex, storeSum);
    scanRootToLeaves(data, stride);// traverse down tree to build the scan 
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

    g_data[address] += uni;
    g_data[address + get_local_size(0)] += (get_local_id(0) + get_local_size(0) < n) * uni;
}

/////////////////////////////////////////////////////////////////////

//      kernels required for join

/////////////////////////////////////////////////////////////////////

// The following kernel is for traditional hash joins (Comment by Yuan)

__kernel void count_hash_num(__global int *dim, long inNum, __global int *num, int hsize) {
    size_t stride = get_global_size(0);
    size_t offset = get_global_id(0);

    for(size_t i=offset;i<inNum;i+=stride) {
        int joinKey = dim[i];
        int hKey = joinKey & (hsize-1);
        atomic_add(&(num[hKey]),1);
    }
}

// The following kernel is for traditional hash joins (Comment by Yuan)

__kernel void build_hash_table(__global int *dim, long inNum, __global int *psum, __global int * bucket, int hsize) {

    size_t stride = get_global_size(0);
    size_t offset = get_global_id(0);

    for(size_t i=offset;i<inNum;i+=stride) {
        int joinKey = dim[i];
        int hKey = joinKey & (hsize-1);
        int pos = atomic_add(&psum[hKey],1) * 2;
        bucket[pos] = joinKey;
        pos += 1;
        int dimId = i+1;
        bucket[pos] = dimId;
    }

}

__kernel void count_join_result(__global int* num, __global int* psum, __global int* bucket, __global int* fact, long inNum, __global int* count, __global int * factFilter,int hsize) {
    int lcount = 0;
    size_t stride = get_global_size(0);
    size_t offset = get_global_id(0);

    for(size_t i=offset;i<inNum;i+=stride) {
        int fkey = fact[i];
        int hkey = fkey &(hsize-1);
        int keyNum = num[hkey];
        int fvalue = 0;

        for(int j=0;j<keyNum;j++) {
            int pSum = psum[hkey];
            int dimKey = bucket[2*j + 2*pSum];
            int dimId = bucket[2*j + 2*pSum + 1];
            if( dimKey == fkey) {
                lcount ++;
                fvalue = dimId;
                break;
            }
        }
        factFilter[i] = fvalue;
    }

    count[offset] = lcount;
}

/////////////////////////////////////////////////////////////////////

//      kernels required for aggregation

/////////////////////////////////////////////////////////////////////

char * gpuStrcpy(char * dst, const char * src) {

    char * orig = dst;
    while (*src)
        *dst++ = *src++;
    *dst = '\0';

    return orig;
}

char* gpuStrncat(char *dest, const char *src, size_t n) {
    int dest_len = 0;
    int i;

    char * tmp = dest;
    while (*tmp != '\0') {
        tmp++;
        dest_len++;
    }

    for (i = 0; i < n && src[i] != '\0'; i++)
        dest[dest_len + i] = src[i];

    dest[dest_len + i] = '\0';
    return dest;
}

char * gpuStrcat(char * dest, const char * src) {
    char *tmp = dest;
    int dest_len = 0;
    int i;

    while (*tmp != '\0') {
        tmp++;
        dest_len++;
    }

    for (i = 0; src[i] != '\0'; i++) {
        dest[dest_len + i] = src[i];
    }

    dest[dest_len + i] = '\0';

    return dest;
}

unsigned int StringHash(const char* s) {
    unsigned int hash = 0;
    int c;

    while ((c = *s++)) {
        hash = ((hash << 5) + hash) ^ c;
    }

    return hash;
}

__kernel void build_groupby_key(__global char * content, __global long * colOffset, int gbColNum, __global int * gbIndex, __global int * gbType, __global int * gbSize, long tupleNum, __global int * key, __global int *num){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);

    for(size_t i = tid; i < tupleNum; i+= stride) {
        int hkey = 0;
        for (int j = 0; j< gbColNum; j++) {
             int index = gbIndex[j];
             long offset = colOffset[index];
 
             if (index == -1){
                 hkey = 1;
 
             } else if (gbType[j] == STRING) {
                 for(int k = 0; k < gbSize[j]; k++)
                     hkey ^= ( hkey << 5 ) + ( hkey >> 2 ) + content[offset + i * gbSize[j] + k];
 
             } else if (gbType[j] == INT){
                 for(int k = 0; k < gbSize[j]; k++) // gbSize[j] for int types should be 4 bytes
                     hkey ^= ( hkey << 5 ) + ( hkey >> 2 ) + content[offset + i * gbSize[j] + k];
             }
        }
        hkey = hkey % HSIZE;
        if (hkey < 0)
           hkey += HSIZE;
        key[i]= hkey;
        num[hkey] = 1;
    }
}

__kernel void count_group_num(__global int *num, int tupleNum, __global int *totalCount){
    size_t stride = get_global_size(0);
    size_t tid = get_global_id(0);
    int localCount = 0;

    for(size_t i = tid; i < tupleNum; i += stride) {
        if(num[i] == 1) {
            localCount++;
        }
    }

    atomic_add(totalCount,localCount);
}

#define declare_join_dim_kernel(column_type)                                                    \
__kernel void join_dim_##column_type                                                            \
(__global int *resPsum, __global column_type * dim                                              \
        , int attrSize, long num, __global int * filter, __global column_type * result) {       \
    size_t startIndex = get_global_id(0);                                                       \
    size_t stride = get_global_size(0);                                                         \
    long localCount = resPsum[startIndex];                                                      \
                                                                                                \
    for(size_t i = startIndex; i < num;i += stride) {                                           \
        int dimId = filter[i];                                                                  \
        if( dimId != 0){                                                                        \
            result[localCount] = dim[dimId - 1];                                                \
            localCount++;                                                                       \
        }                                                                                       \
    }                                                                                           \
}                                                                                               \

declare_join_dim_kernel(int)
declare_join_dim_kernel(long)
declare_join_dim_kernel(float)
declare_join_dim_kernel(double)
declare_join_dim_kernel(boolean)
declare_join_dim_kernel(char)

#define declare_join_fact_kernel(column_type)                                                   \
__kernel void join_fact_##column_type                                                           \
(__global int *resPsum, __global column_type * fact                                             \
        , int attrSize, long  num, __global int * filter, __global column_type * result) {      \
                                                                                                \
    size_t startIndex = get_global_id(0);                                                       \
    size_t stride = get_global_size(0);                                                         \
        long localCount = resPsum[startIndex];                                                  \
                                                                                                \
        for(size_t i=startIndex;i<num;i+=stride){                                               \
                if(filter[i] != 0){                                                             \
                        result[localCount] = fact[i];                                           \
                        localCount ++;                                                          \
                }                                                                               \
        }                                                                                       \
}                                                                                               \

declare_join_fact_kernel(int)
declare_join_fact_kernel(long)
declare_join_fact_kernel(float)
declare_join_fact_kernel(double)
declare_join_fact_kernel(boolean)
declare_join_fact_kernel(char)

float getExp(__global char *content, __global long * colOffset,struct mathExp exp,int pos) {
    float res = 0;
    if(exp.op == NOOP) {
        if (exp.opType == CONS)
        res = exp.opValue;
        else if (exp.opType == COLUMN) {
            int index = exp.opValue;
            res = ((__global int *)(content+colOffset[index]))[pos];
        } else {
            // raise an exception or set an error code.
        }
    } else {
        // raise an exception or set an erro code
    }
    return res;
}

float calMathExp(__global char *content, __global long * colOffset,struct mathExp exp, __global struct mathExp *mexp, int pos) {
    float res;

    if(exp.op == NOOP) {
        if (exp.opType == CONS)
        res = exp.opValue;
        else {
            int index = exp.opValue;
            res = ((__global int *)(content+colOffset[index]))[pos];
        }

    } else if(exp.op == PLUS ) {
        res = getExp(content,colOffset,mexp[2*pos],pos) + getExp(content, colOffset,mexp[2*pos+1],pos);

    } else if (exp.op == MINUS) {
        res = getExp(content,colOffset,mexp[2*pos],pos) - getExp(content, colOffset,mexp[2*pos+1],pos);

    } else if (exp.op == MULTIPLY) {
        res = getExp(content,colOffset,mexp[2*pos],pos) * getExp(content, colOffset,mexp[2*pos+1], pos);

    } else if (exp.op == DIVIDE) {
        res = getExp(content,colOffset,mexp[2*pos],pos) / getExp(content, colOffset,mexp[2*pos+1],pos);
    } else {
        // raise an exception or set a error code or terminate execution
    }

    return res;
}

__kernel void agg_cal(__global char * content, __global long *colOffset, int colNum, __global struct mathExp* exp, __global struct mathExp *mexp, __global int * gbType, __global int * gbSize, long tupleNum, __global int * key, __global int *psum, __global char * result, __global long * resOffset, __global int *gbFunc) {
//__kernel void agg_cal(__global char * content, __global long *colOffset, int colNum, __global char* expRaw, __global char *mexpRaw, __global int * gbType, __global int * gbSize, long tupleNum, __global int * key, __global int *psum, __global char * result, __global long * resOffset, __global int *gbFunc) {

    size_t stride = get_global_size(0);
    size_t index = get_global_id(0);

    for(int i=index; i < tupleNum; i += stride) {

        int hKey = key[i];
        int offset = psum[hKey];

        for(int j=0; j <colNum; j++) {
            int func = gbFunc[j];
            if(func == GROUPBY) {
                int type = exp[j].opType;
                int attrSize = gbSize[j];

                if (type == CONS) {
                    int value = exp[j].opValue;
                    char * buf = (char *) &value;
                    for(int k=0; k < attrSize; k++) {
                        result[resOffset[j] + offset*attrSize +k] = buf[k];
                    }
                } else if (type == COLUMN) {
                    int index = exp[j].opValue;
                    for(int k=0; k < attrSize; k++) {
                        result[resOffset[j] + offset*attrSize +k] = content[colOffset[index] + i*attrSize + k];
                    }
                } else {
                    // FIXME raise an exception here or stop execution or set an error code.
                }
            } else if (func == SUM) {
                float tmpRes = calMathExp(content, colOffset, exp[j], mexp, i);
                AtomicAdd(& ((__global float *)(result + resOffset[j]))[offset], tmpRes);
            } else if (func == MIN) {
                float tmpRes = calMathExp(content, colOffset, exp[j], mexp, i);
                AtomicMin(& ((__global float *)(result + resOffset[j]))[offset], tmpRes);
            } else if (func == MAX) {
                float tmpRes = calMathExp(content, colOffset, exp[j], mexp, i);
                AtomicMax(& ((__global float *)(result + resOffset[j]))[offset], tmpRes);
            } else if (func == COUNT) {
                int tmpRes = 1.0;
                atomic_add(& ((__global int *)(result + resOffset[j]))[offset], tmpRes);
            } else if (func == AVG) {
                float tmpRes = calMathExp(content, colOffset, exp[j], mexp, i);
                AtomicAdd(& ((__global float *)(result + resOffset[j]))[offset], tmpRes);
            } else {
                // FIXME raise an exception here or stop execution or set an error code.
            }
        }
    }
}

