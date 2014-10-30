package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class GpuFilteredPartition[T <: Product : ClassTag, U: ClassTag : TypeTag]
(context: OpenCLContext, columnTypes: Array[String], colIndex: Int,
 operation: ComparisonOperation.Value, value: U, capacity: Int)
  extends GpuPartition[T](context, columnTypes, capacity) {

  override def fill(iter: Iterator[T]): Unit = {
    val startTransformDataTime = System.nanoTime
    super.fill(iter)
    val endTransformDataTime = System.nanoTime
    val startSelectionTotalTime = System.nanoTime
    val resultSize = filter[U](colIndex, value, operation)

    intData.zipWithIndex.filter(_._1 != null).foreach({
      case (inData: Array[Int], index) => {
        if (index != colIndex) {
          project(intData(colIndex), size, intData(colIndex), resultSize)
        }
      }
    })
    size = resultSize
    val endSelectionTotalTime = System.nanoTime

    val totalTime = endSelectionTotalTime - startTransformDataTime
    println("Test with size=%,12d".format(size))
    println("Total transform time (ns) to copy %,12d elements of data = %,12d".format(-1, endTransformDataTime - startTransformDataTime))
    println("Selection time (ns) = %,12d".format(endSelectionTotalTime - startSelectionTotalTime))
    println("Total selection time (ns) = %,12d".format(totalTime))
  }

}
