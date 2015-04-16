package org.apache.spark.gpu

import org.apache.spark.rdd._

/**
 * Created by fathi on 12/02/14.
 */
class GpuSortPartitionSuit extends GpuSuit {

  override val DEFAULT_CAPACITY = 20

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

    val sortPartition: GpuSortPartition[(Int, Int)] = new GpuSortPartition[(Int, Int)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11, 1), (12, 2), (11, 3), (12, 4), (11, 5))

    validateResults[(Int, Int)](expectedData, Array(sortPartition))
  }

  test("GpuSortPartitionSuit (Int, Int) with duplicate keys") {
    val testData: IndexedSeq[(Int, Int)] = Array((11, 1), (12, 1), (11, 2), (12, 2), (11, 3))

    val partition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition: GpuSortPartition[(Int, Int)] = new GpuSortPartition[(Int, Int)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11, 1), (12, 1), (12, 2), (11, 2), (11, 3))

    validateResults[(Int, Int)](expectedData, Array(sortPartition))
  }

  ignore("GpuSortPartitionSuit (Float, Float)") {
    val testData: IndexedSeq[(Float, Float)] = Array((11, 1), (12, 1), (11, 2), (12, 2), (11, 3))
      .map {
      case (v1, v2) => (v1.toFloat, v2.toFloat)
    }
    val partition = new GpuPartition[(Float, Float)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition: GpuSortPartition[(Float, Float)] = new GpuSortPartition[(Float, Float)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11f, 1f), (12f, 1f), (12f, 2f), (11f, 2f), (11f, 1f))

    validateResults[(Float, Float)](expectedData, Array(sortPartition))
  }

  ignore("GpuSortPartitionSuit (Float, Float) with duplicate keys") {
    val testData: IndexedSeq[(Float, Float)] = Array((11, 1), (12, 1), (11, 2), (12, 2), (11, 3))
      .map {
      case (v1, v2) => (v1.toFloat, v2.toFloat)
    }
    val partition = new GpuPartition[(Float, Float)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition: GpuSortPartition[(Float, Float)] = new GpuSortPartition[(Float, Float)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11f, 1f), (12f, 1f), (12f, 2f), (11f, 2f), (11f, 3f))

    validateResults[(Float, Float)](expectedData, Array(sortPartition))
  }

  test("GpuSortPartitionSuit (String, String)") {
    val testData: IndexedSeq[(String, String)] = Array((11, 3), (11, 1), (11, 5), (12, 2), (12, 4))
      .map {
      case (v1, v2) => (s"COL_1_STR_${v1}", s"COL_2_STR_${v2}")
    }

    val partition = new GpuPartition[(String, String)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition: GpuSortPartition[(String, String)] = new GpuSortPartition[(String, String)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array((11, 3), (12, 4), (11, 5), (11, 1), (12, 2)).map {
      case (v1, v2) => (s"COL_1_STR_${v1}", s"COL_2_STR_${v2}")
    }

    validateResults[(String, String)](expectedData, Array(sortPartition))
  }

  test("GpuSortPartitionSuit (String, String) with duplicate keys") {
    val testData: IndexedSeq[(String, String)] = Array((11, 1), (12, 1), (11, 2), (12, 2), (11, 3))
      .map {
      case (v1, v2) => (s"COL_1_STR_${v1}", s"COL_2_STR_${v2}")
    }

    val partition = new GpuPartition[(String, String)](openCLContext, DEFAULT_CAPACITY)

    partition.fill(testData.toIterator)

    val sortPartition: GpuSortPartition[(String, String)] = new GpuSortPartition[(String, String)](
      openCLContext,
      partition,
      Array(1), // sort col indexes
      Array(SortDirection.ASCENDING, SortDirection.ASCENDING), // sort direction
      DEFAULT_CAPACITY)

    sortPartition.sort()
    val expectedData = Array(("COL_1_STR_11", "COL_2_STR_1"), ("COL_1_STR_11", "COL_2_STR_3"),
      ("COL_1_STR_11", "COL_2_STR_2"), ("COL_1_STR_12", "COL_2_STR_1"), ("COL_1_STR_12", "COL_2_STR_2"))

    validateResults[(String, String)](expectedData, Array(sortPartition))
  }

}
