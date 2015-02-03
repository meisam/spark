package org.apache.spark.rdd

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}


/**
 * Created by fathi on 10/1/14.
 */
class GpuFilteredRDD[T <: Product: ClassTag : TypeTag, V: TypeTag]
(prev: GpuRDD[T], colIndex: Int, operation: ComparisonOperation.Value, value: V,
 chunkCapacity: Int)
  extends GpuRDD[T](prev, chunkCapacity) {

//  val mirror = ru.runtimeMirror(getClass.getClassLoader)
//
//  implicit val xClassTag = ClassTag[T](mirror.runtimeClass(typeOf[T]))

  override def compute(split: Partition, context: TaskContext): Iterator[GpuPartition[T]]
  = {
    new GpuFilteredPartitionIterator(firstParent[GpuPartition[T]].iterator(split, context),
      openCLContext, colIndex, operation, value, chunkCapacity)
  }
}
