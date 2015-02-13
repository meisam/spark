package org.apache.spark.gpu

import org.apache.spark.rdd._
import org.apache.spark.scheduler.OpenCLContext

/**
 * Created by fathi on 12/02/14.
 */
class GpuSortPartitionSuit extends GpuSuit {

  val DEFAULT_CAPACITY = (1 << 5)
  val openCLContext = new OpenCLContext

  val times2MathExo = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
  val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
  val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
  val countMathExp = new MathExp(MathOp.PLUS, 1, times2MathExo, null, MathOperationType.const, 15)

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("GpuSortPartitionSuit (Int, Int)") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 3), (11, 1), (11, 5), (12, 2), (12, 4))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition = new GpuSortPartition[(Int, Int), (Int, Int)](openCLContext,
      partition,
      Array(1, 0), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11, 1), (12, 2), (11, 3), (12, 4), (11, 5))

    validateResults[(Int, Int)](expectedData, Array(sortPartition))
  }

}
