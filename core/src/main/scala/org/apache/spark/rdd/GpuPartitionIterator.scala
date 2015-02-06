package org.apache.spark.rdd

import org.apache.spark.Logging

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.reflect.runtime.{universe => ru}

class GpuPartitionIterator[T <: Product : TypeTag]
(itr: Iterator[T], capacity: Int = 1 << 20)
  extends Serializable with Iterator[GpuPartition[T]] with Logging{

  implicit val ct = ClassTag[T](ru.runtimeMirror(getClass.getClassLoader).runtimeClass(typeOf[T]))
  val data: Array[T] = itr.toArray(ct)
  var currentPosition = 0

  override def next(): GpuPartition[T] = {
    if (currentPosition >= data.length) {
      throw new ArrayIndexOutOfBoundsException(f"${currentPosition} >= ${data.length}")
    }
    val slice = data.slice(currentPosition, currentPosition + capacity)
    currentPosition += slice.length
    val currentPartition = new GpuPartition[T](null, capacity)
    currentPartition.fill(slice.toIterator)
    currentPartition
  }

  override def hasNext: Boolean = {
    currentPosition < data.length
  }
}