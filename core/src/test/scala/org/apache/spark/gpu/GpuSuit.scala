package org.apache.spark.gpu


import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.GpuPartition
import org.apache.spark.scheduler.OpenCLContextSingletone
import org.scalatest.FunSuite

import scala.language.existentials
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * A parent class for all GPU test
 */
class GpuSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 10)

  val openCLContext = OpenCLContextSingletone.openClContext

  protected def flattenResults[T <: Product : TypeTag : ClassTag](collectedPartitions:
    Array[GpuPartition[T]]): Array[T] = {

    val collectedData: Array[T] = collectedPartitions.map { p =>
      (0 until p.size).map(i => p(i))
    }.flatten[T]
    collectedData
  }

  def validateResults[T <: Product : TypeTag : ClassTag](expectedData: Array[T], collectedPartitions: Array[GpuPartition[T]]):
  Unit = {
    val totalResult = collectedPartitions.map(p => p.size).reduce(_ + _)
    assert(totalResult === expectedData.length)

    val collectedData = flattenResults[T](collectedPartitions)
    expectedData.zip(collectedData).foreach {
      case (vt, vc) =>
        assert(vt === vc)
    }

  }
}
