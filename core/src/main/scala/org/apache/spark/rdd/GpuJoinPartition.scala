package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext
import org.jocl.CL._
import org.jocl._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class GpuJoinPartition[T <: Product : ClassTag, T2 <: Product : ClassTag : TypeTag,
U: ClassTag : TypeTag]
(context: OpenCLContext, columnTypes: Array[String], rightPartition: GpuPartition[T2],
 joinColIndexLeft: Int, joinColIndexRight: Int, capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {

}
