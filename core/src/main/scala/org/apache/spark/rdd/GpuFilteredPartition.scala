package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class GpuFilteredPartition[T <: Product : ClassTag, U: ClassTag : TypeTag]
(context: OpenCLContext, columnTypes: Array[String], colIndex: Int,
 operation: ComparisonOperation.Value, value: U, capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {

  override def fill(iter: Iterator[T]): Unit = {
    super.fill(iter)
    val resultSize = filter[U](colIndex, value, operation)

    intData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Int](colIndex, resultSize)
        }
      }
    })
    longData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Long](colIndex, resultSize)
        }
      }
    })
    floatData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Float](colIndex, resultSize)
        }
      }
    })
    doubleData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Double](colIndex, resultSize)
        }
      }
    })
    booleanData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Boolean](colIndex, resultSize)
        }
      }
    })
    charData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[Char](colIndex, resultSize)
        }
      }
    })
    stringData.zipWithIndex.filter(_._1 != null).foreach({
      case (_, index) => {
        if (index != colIndex) {
          project[String](colIndex, resultSize)
        }
      }
    })
    size = resultSize
  }

}
