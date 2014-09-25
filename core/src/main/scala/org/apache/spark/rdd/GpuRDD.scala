/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.lang.reflect.{Array => JArray}

import org.apache.spark.{Logging, Partition, TaskContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 *
 */
class GpuRDD[T: ClassTag](prev: RDD[T]) extends RDD[RDDChuck[T]](prev) {
  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[RDDChuck[T]] = {
    throw new NotImplementedError("org.apache.spark.rdd.GpuRDD.compute is not implemented yet.")
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions

}

class RDDChuck[T: TypeTag] extends Logging {

  def MAX_SIZE: Int = (1 << 20)

  val rawData = initArray


  def typeInfo() = {
    val tag = typeTag[T]
    tag
  }

  def initArray(): AnyRef = {

    logError("org/apache/spark/rdd/GpuRDD.scala:52 is running")

    println("Chunk is %s".format(typeInfo /*.runtimeClass*/))
    println("Type of Chunk is %s".format(typeInfo.getClass /*.runtimeClass*/))
    val tag = typeInfo
    tag.tpe match {
      case TypeRef(x, y, args) => {
        println(runtimeMirror( getClass.getClassLoader ).runtimeClass(x))
        y.fullName match {
          case "scala.Tuple5" => {
            args.foreach(t => {

            })

          }
        }
      }

    }

    throw new NotImplementedError("org/apache/spark/rdd/GpuRDD.scala:70 is not implemented yet")

    /*    val d = this.typeInfo() match {
          case t: TypeTag[Product] => {

            (JArray.newInstance(t.tpegetDeclaredFields()(0).getType, MAX_SIZE),
              JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
              JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE))
          }
        case t: Class[Tuple1[_]] => {
          JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE)
        }
        case t: Class[Tuple2[_, _]] => {
            (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE))
        }
        case t: Class[Tuple4[_, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE))
        }
        case t: Class[Tuple5[_, _, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(4).getType, MAX_SIZE))
        }
        case t: Class[Tuple6[_, _, _, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(4).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(5).getType, MAX_SIZE))
        }
        case t: Class[Tuple7[_, _, _, _, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(4).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(5).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(6).getType, MAX_SIZE))
        }
        case t: Class[Tuple8[_, _, _, _, _, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(4).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(5).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(6).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(7).getType, MAX_SIZE))
        }
        case t: Class[Tuple9[_, _, _, _, _, _, _, _, _]] => {
          (JArray.newInstance(t.getDeclaredFields()(0).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(1).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(2).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(3).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(4).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(5).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(6).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(7).getType, MAX_SIZE),
            JArray.newInstance(t.getDeclaredFields()(8).getType, MAX_SIZE))
        }
          case _ => throw
        new NotImplementedError("org.apache.spark.rdd.RDDChuck.initArray " +
        "org/apache/spark/rdd/GpuRDD.scala:126" +
        " is not implemented yet")
      }
        d
        */
  }

  def apply(i: Int): T = {
    throw new NotImplementedError("org.apache.spark.rdd.RDDChuck.apply is not implemented yet")

    /*
  genericType match {
  case _: Tuple1 => {
    val arr = rawData.asInstanceOf[Array[Array[Int]]]
    (arr(0)(i)).asInstanceOf[T]
    }
    case _: Tuple2 => (rawData(0)(i), rawData(1)(i)).asInstanceOf[T]
    case _: Tuple3 => (rawData(0)(i), rawData(1)(i), rawData(2)(i)).asInstanceOf[T]
    case _: Tuple4 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i))
      .asInstanceOf[T]
    case _: Tuple5 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i)).asInstanceOf[T]
    case _: Tuple6 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i)).asInstanceOf[T]
    case _: Tuple7 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i)).asInstanceOf[T]
    case _: Tuple8 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i)).asInstanceOf[T]
    case _: Tuple9 => (rawData(0)(i), rawData(1)(i), rawData(2)(i), rawData(3)(i),
      rawData(4)(i), rawData(5)(i), rawData(6)(i), rawData(7)(i), rawData(9)(i)).asInstanceOf[T]
      case _ =>
    throw new IllegalArgumentException("%s is and unknown element type".format(classOf[T]))
  }
    */
  }

}
