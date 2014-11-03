package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag

class GpuAggregationPartition[T <: Product : ClassTag]
(context: OpenCLContext, columnTypes: Array[String], groupByColumnIndexes: Array[Int],
 aggregations: Array[AggregationOperation.Value], capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {
  //
  def aggregate(): Unit = {
  }

  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    this.aggregate()
  }
}

object AggregationOperation extends Enumeration {

  def count = Value("count")

  def min = Value("min")

  def max = Value("max")

  def sum = Value("sum")

  def avg = Value("avg")
}