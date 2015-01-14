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

import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.AggregationExp
import org.apache.spark.rdd.AggregationOperation
import org.apache.spark.rdd.ComparisonOperation
import org.apache.spark.rdd.GpuAggregationPartition
import org.apache.spark.rdd.GpuFilteredPartition
import org.apache.spark.rdd.GpuJoinPartition
import org.apache.spark.rdd.GpuJoinPartition
import org.apache.spark.rdd.GpuPartition
import org.apache.spark.rdd.MathExp
import org.apache.spark.rdd.MathOp
import org.apache.spark.rdd.MathOperationType
import org.apache.spark.scheduler.OpenCLContext
import org.scalatest.FunSuite

//PART
// 0 P_PARTKEY:INTEGER
// 1 P_NAME:TEXT
// 2 P_MFGR:TEXT
// 3 P_CATEGORY:TEXT
// 4 P_BRAND1:TEXT
// 5 P_COLOR:TEXT
// 6 P_TYPE:TEXT
// 7 P_SIZE:INTEGER
// 8 P_CONTAINER:TEXT

//SUPPLIER
// 0 S_SUPPKEY:INTEGER
// 1 S_NAME:TEXT
// 2 S_ADDRESS:TEXT
// 3 S_CITY:TEXT
// 4 S_NATION:TEXT
// 5 S_REGION:TEXT
// 6 S_PHONE:TEXT

//CUSTOMER
// 0 C_CUSTKEY:INTEGER
// 1 C_NAME:TEXT
// 2 C_ADDRESS:TEXT
// 3 C_CITY:TEXT
// 4 C_NATION:TEXT
// 5 C_REGION:TEXT
// 6 C_PHONE:TEXT
// 7 C_MKTSEGMENT:TEXT

//LINEORDER
// 0 LO_ORDERKEY:INTEGER
// 1 LO_LINENUMBER:INTEGER
// 2 LO_CUSTKEY:INTEGER
// 3 LO_PARTKEY:INTEGER
// 4 LO_SUPPKEY:INTEGER
// 5 LO_ORDERDATE:DATE
// 6 LO_ORDERPRIORITY:TEXT
// 7 LO_SHIPPRIORITY:TEXT
// 8 LO_QUANTITY:INTEGER
// 9 LO_EXTENDEDPRICE:DECIMAL
//10 LO_ORDTOTALPRICE:DECIMAL
//11 LO_DISCOUNT:INTEGER
//12 LO_REVENUE:DECIMAL
//13 LO_SUPPLYCOST:DECIMAL
//14 LO_TAX:INTEGER
//15 L_COMMITDATE:DATE
//16 L_SHIPMODE:TEXT

//DDATE
// 0 D_DATEKEY:DATE
// 1 D_DATE:TEXT
// 2 D_DAYOFWEEK:TEXT
// 3 D_MONTH:TEXT
// 4 D_YEAR:INTEGER
// 5 D_YEARMONTHNUM:INTEGER
// 6 D_YEARMONTH:TEXT
// 7 D_DAYNUMINWEEK:INTEGER
// 8 D_DAYNUMINMONTH:INTEGER
// 9 D_DAYNUMINYEAR:INTEGER
//10 D_MONTHNUMINYEAR:INTEGER
//11 D_WEEKNUMINYEAR:INTEGER
//12 D_SELLINGSEASON:TEXT
//13 D_LASTDAYINWEEKFL:TEXT
//14 D_LASTDAYINMONTHFL:TEXT
//15 D_HOLIDAYFL:TEXT
//16 D_WEEKDAYFL:TEXT

/**
 * A set of test cases that run SSB on Spark GPU
 */
class GpuSsbQueriesSuit extends FunSuite with SharedSparkContext {

  val DEFAULT_CAPACITY = (1 << 22)
  val openCLContext = new OpenCLContext

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  test("SSB_q1_1 test") {
    // select sum(lo_extendedprice*lo_discount) as revenue
    // from lineorder, ddate
    // where lo_orderdate = d_datekey
    // and d_year = 1993 and lo_discount>=1
    // and lo_discount<=3
    // and lo_quantity<25;

    /*__________________________ d_datekey. d_year _____________________________*/
    val ddatePartition = new GpuPartition[(Int, Int)](openCLContext, DEFAULT_CAPACITY)
    ddatePartition.fillFromFiles(Array("/home/fathi/workspace/gpudb/src/utility/DDATE0", "/home/fathi/workspace/gpudb/src/utility/DDATE4"))
    println("ddateTable.size = %,d".format(ddatePartition.size))

    /*____________________________________price,dscnt,date, qty_________________________________*/
    val lineorderPartition = new GpuPartition[(Float, Int, Int, Int)](openCLContext, DEFAULT_CAPACITY)
    lineorderPartition.fillFromFiles(Array(
      "/home/fathi/workspace/gpudb/src/utility/LINEORDER9",
      "/home/fathi/workspace/gpudb/src/utility/LINEORDER11",
      "/home/fathi/workspace/gpudb/src/utility/LINEORDER5",
      "/home/fathi/workspace/gpudb/src/utility/LINEORDER8"))

    println("lineorderTable.size = %,d".format(lineorderPartition.size))

    val loFilterQty = new GpuFilteredPartition[(Float, Int, Int, Int), Int](openCLContext, lineorderPartition, 3, ComparisonOperation.<, 25, DEFAULT_CAPACITY)
    loFilterQty.filter()
    println("loFilterQty.size = %,d".format(loFilterQty.size))

    val loFilterDiscount1 = new GpuFilteredPartition[(Float, Int, Int, Int), Int](openCLContext, loFilterQty, 1, ComparisonOperation.<=, 3, DEFAULT_CAPACITY)
    loFilterDiscount1.filter()
    println("loFilterDiscount1.size = %,d".format(loFilterDiscount1.size))

    val loFilterDiscount2 = new GpuFilteredPartition[(Float, Int, Int, Int), Int](openCLContext, loFilterDiscount1, 1, ComparisonOperation.>=, 1, DEFAULT_CAPACITY)
    loFilterDiscount2.filter()
    println("loFilterDiscount2.size = %,d".format(loFilterDiscount2.size))

    val joinPartition = new GpuJoinPartition[(Float, Int, Int, Int, Int), (Float, Int, Int, Int), (Int, Int), Int](
      openCLContext, loFilterDiscount2, ddatePartition, 2, 0, DEFAULT_CAPACITY)
    joinPartition.join()
    println("joinPartition.size = %,d".format(joinPartition.size))

    val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)

    val aggregations = Array(new AggregationExp(AggregationOperation.sum, col0))
    val aggPartitoin = new GpuAggregationPartition[Tuple1[Float], (Float, Int, Int, Int, Int)](
      openCLContext, joinPartition, aggregations, DEFAULT_CAPACITY)
    aggPartitoin.aggregate()
    println("aggPartitoin.size = %,d".format(aggPartitoin.size))

  }
}