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

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import scala.language.existentials
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.rdd.MathExp
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.nio.ByteBuffer


/**
 *
 */
class ScalaSpecsSuit extends FunSuite with SharedSparkContext {


  override def afterAll(): Unit = {
    // maybe release resources
  }
  
  test("String hash") {
    def stringHash(s: String): Int = {

    var hash = 0

    s.foreach { c => 
        hash = ((hash << 5) + hash) ^ c;
    }

    hash
   }

    
    assert(stringHash("111") === 53841)
    assert(stringHash("112") === 1)
  }
  

  test("scanLeft test") {
    val testData = Array(1, 5, -4, 0, 1)

    val actualResults = testData.scanLeft(0)(_ + _)

    val expectedResults = Array(0, 1, 6, 2, 2, 3)

    assert(expectedResults.length === actualResults.length)
    assert(expectedResults.length === testData.length + 1)


    expectedResults.zip(expectedResults).foreach { case (expected, actual) =>
      assert(expected === actual)
    }
  }

  test("TypeTag test") {
    assert(genericMethod(1) === implicitly[TypeTag[Int]].tpe)
    assert(genericMethod(1d) === implicitly[TypeTag[Double]].tpe)
    assert(genericMethod(1f) === implicitly[TypeTag[Float]].tpe)
  }

  def genericMethod[T: TypeTag](a: T): ru.Type = {
    val classLoader = this.getClass.getClassLoader
    val mirror = ru.runtimeMirror(classLoader)
    implicitly[TypeTag[T]].tpe
  }
  
  test("raw MathExp serialize") {
    val exp1 = new MathExp
   val byteBuffer = ByteBuffer.wrap(new Array[Byte](exp1.size))
//    out.writeObject(exp1)
    exp1.writeTo(byteBuffer)
    
    val bytes = byteBuffer.array()
    
    println("bytes = %s".format(bytes.mkString(",")))
  }

}

