package org.apache.spark.rdd

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.ClassTag

class GpuFilteredPartition[T <: Product : ClassTag]
(context: OpenCLContext, columnTypes: Array[String], colIndex: Int, operation: Int, value: Int,
 capacity: Int) extends GpuPartition[T](context, columnTypes, capacity) {
  override def fill(iter: Iterator[T]): Unit = {
    val startTransformDataTime = System.nanoTime
    val endTransformDataTime = System.nanoTime
    super.fill(iter: Iterator[T])
    val startSelectionTotalTime = System.nanoTime

    if (columnTypes(colIndex) == "INT") {
      val resultSize = compute(intData(colIndex), size.toLong, value, operation)

      size = resultSize
      intData.zipWithIndex.filter(_._1 != null).foreach({
        case (inData: Array[Int], index) => {
          if (index != colIndex) {
            project(intData(colIndex), size, intData(colIndex), resultSize)
          }
        }
      })
      size = resultSize
    }
    val endSelectionTotalTime = System.nanoTime

    val totalTime = endSelectionTotalTime - startTransformDataTime
    println("Test with size=%,12d".format(size))
    println("Total transform time (ns) to copy %,12d elements of data = %,12d".format(-1, endTransformDataTime - startTransformDataTime))
    println("Selection time (ns) = %,12d".format(endSelectionTotalTime - startSelectionTotalTime))
    println("Total selection time (ns) = %,12d".format(totalTime))
  }

}
