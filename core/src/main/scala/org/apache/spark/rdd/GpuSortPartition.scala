package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag
;

class GpuSortPartition[T <: Product : ClassTag]
(context: OpenCLContext, columnTypes: Array[String], groupByColumnIndexes: Array[Int],
 aggregations: Array[AggregationOperation.Value], capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {

  def sort(): Unit = {

  }


  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    this.sort()
  }
}
