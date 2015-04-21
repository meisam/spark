package org.apache.spark.rdd

import java.nio._

import org.apache.spark.scheduler.OpenCLContext

import scala.reflect.runtime.universe.{TypeTag, typeOf}

class GpuProjectionPartition[T <: Product : TypeTag, TP <: Product : TypeTag](context: OpenCLContext,
  parent: GpuPartition[TP], projectColumnIndices: Array[Int], capacity: Int)
  extends GpuPartition[T](context, capacity) {

  override def byteData = {
    if (_byteData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Byte.tpe).map(_._2)
      _byteData = new Array[ByteBuffer](colIndexes.length)

      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Byte])
      assert(typedIndices.length == colIndexes.length)

      _byteData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _byteData(thisIndex) = parent.byteData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _byteData
  }

  override def shortData = {
    if (_shortData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Short.tpe).map(_._2)
      _shortData = new Array[ShortBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Short])
      assert(typedIndices.length == colIndexes.length)

      _shortData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _shortData(thisIndex) = parent.shortData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _shortData
  }

  override def intData = {
    if (_intData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Int.tpe).map(_._2)
      _intData = new Array[IntBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Int])
      assert(typedIndices.length == colIndexes.length)

      _intData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _intData(thisIndex) = parent.intData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _intData
  }

  override def longData = {
    if (_longData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Long.tpe).map(_._2)
      _longData = new Array[LongBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Long])
      assert(typedIndices.length == colIndexes.length)

      _longData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _longData(thisIndex) = parent.longData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _longData
  }

  override def floatData = {
    if (_floatData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Float.tpe).map(_._2)
      _floatData = new Array[FloatBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Float])
      assert(typedIndices.length == colIndexes.length)

      _floatData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _floatData(thisIndex) = parent.floatData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _floatData
  }

  override def doubleData = {
    if (_doubleData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Double.tpe).map(_._2)
      _doubleData = new Array[DoubleBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Double])
      assert(typedIndices.length == colIndexes.length)

      _doubleData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _doubleData(thisIndex) = parent._doubleData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _doubleData
  }

  override def booleanData = {
    if (_booleanData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Boolean.tpe).map(_._2)
      _booleanData = new Array[ByteBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Boolean])
      assert(typedIndices.length == colIndexes.length)

      _booleanData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _booleanData(thisIndex) = parent.booleanData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _booleanData
  }

  override def charData = {
    if (_charData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter(_._1.tpe =:= TypeTag.Char.tpe).map(_._2)
      _charData = new Array[CharBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter(parent.columnTypes(_).tpe =:= typeOf[Char])
      assert(typedIndices.length == colIndexes.length)

      _charData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _charData(thisIndex) = parent.charData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _charData
  }

  override def stringData = {
    if (_stringData == null) {
      val colIndexes = columnTypes.zipWithIndex.filter { case (tp, index) => {
          isStringType(tp)
        }
      }.map { case (tp, index) => index }
      _stringData = new Array[ByteBuffer](colIndexes.length)
      val typedIndices = projectColumnIndices.filter( i => isStringType(parent.columnTypes(i)))
      assert(typedIndices.length == colIndexes.length)

      _stringData.indices.zip(typedIndices).foreach { case (thisIndex, parentIndex) =>
        _stringData(thisIndex) = parent.stringData(parent.toTypeAwareColumnIndex(parentIndex))
      }
    }
    _stringData
  }

  def project(): Int = {
    val startTime = System.nanoTime()
    this.size = parent.size
    inferBestWorkGroupSize()
    val endTime = System.nanoTime()
    logInfo(f"projection time = ${endTime - startTime}%,d")
    this.size
  }

}
