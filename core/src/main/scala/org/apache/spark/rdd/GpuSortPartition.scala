package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.runtime.universe.TypeTag

class GpuSortPartition[T <: Product : TypeTag]
(context: OpenCLContext, idx:Int, groupByColumnIndexes: Array[Int],
 aggregations: Array[AggregationOperation.Value], capacity: Int)
  extends GpuPartition[T](context, idx, capacity) {

  def sort(): Unit = {

  }


  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    this.sort()
  }
}
