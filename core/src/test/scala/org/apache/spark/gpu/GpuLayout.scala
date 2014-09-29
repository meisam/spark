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

package org.apache.spark.gpu

import java.lang.reflect.{TypeVariable, ParameterizedType, Field}

import org.apache.spark.rdd.RDDChuck
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.language.existentials
import scala.reflect.ClassTag

/**
 *
 */
class GpuLayout extends FunSuite with BeforeAndAfterAll {

  class ParametricClass[T: ClassTag](var element: Array[T]) {

    def typeInfo(): Class[_] = {
      val tag: ClassTag[T] = reflect.classTag[T]
      tag.runtimeClass
    }

  }

  test("org.apache.spark.rdd.RDDChuck.initArray test") {
    val x = new RDDChuck[(Int, String, Float, Double, String)]
    assert(x.rawData !== null)
    assert(x.rawData.getClass === null)
  }
}
