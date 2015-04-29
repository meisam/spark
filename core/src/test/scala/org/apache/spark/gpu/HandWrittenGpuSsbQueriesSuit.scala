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

import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.rdd._

import scala.language.existentials

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
// 9 LO_EXTENDEDPRICE:DECIMAL --> INTEGER
//10 LO_ORDTOTALPRICE:DECIMAL --> INTEGER
//11 LO_DISCOUNT:INTEGER
//12 LO_REVENUE:DECIMAL --> INTEGER
//13 LO_SUPPLYCOST:DECIMAL --> INTEGER
//14 LO_TAX:INTEGER --> INTEGER
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
class HandWrittenGpuSsbQueriesSuit extends GpuSuit with Logging{

  override def beforeAll() {
    super.beforeAll()
    //    setLogLevel(LogLevel.LOG_TRACE)
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")
  }

  def measureTime(measurementName: String)(code: => Int): Long = {
    println(f"started $measurementName ...")
    val startTime = System.nanoTime
    val resultsCount = code
    val endTime = System.nanoTime
    val totalTime = endTime - startTime
    println(f"$measurementName time =$totalTime%,d (ns)")
    println(f"$measurementName results =$resultsCount%,d")
    totalTime
  }

  println(f"[PROFILING RESULTS]:Iteration,QueryName,ExecTime,ResultRowCount,ScaleFactor,Comments")

  override val DEFAULT_CAPACITY = 1 << 26

  val ITERATIONS = 5

  val SCALE_FACTOR = 10

  ignore("SSB_q1_1 test") {
    // select sum(lo_extendedprice*lo_discount) as revenue
    // from lineorder, ddate
    // where lo_orderdate = d_datekey
    // and d_year = 1993
    // and lo_discount>=1
    // and lo_discount<=3
    // and lo_quantity<25

    /*__________________________ d_datekey. d_year _____________________________*/

    val startTime = System.nanoTime()

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")

    val dDateRaw = new GpuPartition[(Int, Int)](openCLContext, 3000)
    measureTime("date load") {
      dDateRaw.fillFromFiles(ddatePaths, 0)
    }

    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    //            /*____________________________________price,dscnt,date, qty_________________________________*/
    val loRaw = new GpuPartition[(Int, Int, Int, Int)](openCLContext, DEFAULT_CAPACITY)

    measureTime("lineorder load") {
      loRaw.fillFromFiles(lineOrderPaths)
    }

    val filteredDatesPartition: GpuFilteredPartition[(Int, Int), Int] =
      new GpuFilteredPartition[(Int, Int), Int](openCLContext, 1, 1, ComparisonOperation.==, 1993,
        DEFAULT_CAPACITY)

    val loFilterQty = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      11, 3, ComparisonOperation.<, 25, DEFAULT_CAPACITY)

    val loFilterDiscount1 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext, 12,
      1, ComparisonOperation.<=, 3, DEFAULT_CAPACITY)

    val loFilterDiscount2 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext, 13,
      1, ComparisonOperation.>=, 1, DEFAULT_CAPACITY >> 3)

    val joinPartition = new GpuJoinPartition[(Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int),
      Int](openCLContext, loFilterDiscount2, filteredDatesPartition, 2, 0, DEFAULT_CAPACITY >> 3)

    val projectedPartition = new GpuProjectionPartition[(Int, Int), (Int, Int, Int, Int, Int)](
      openCLContext, joinPartition, Array(0, 1), DEFAULT_CAPACITY >> 4)

    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
    val revenueAggregation = new AggregationExp(AggregationOperation.sum, revenueExp)

    val aggregatePartition = new GpuAggregationPartition[(Float, Float), (Int, Int)](
      openCLContext, projectedPartition, Array(constColumnGroupBy, revenueAggregation), 10)

    //
    //
    (0 until ITERATIONS).foreach { iteration =>

      measureTime("date filter") {
        filteredDatesPartition.filter(dDateRaw)
      }
      measureTime("lineorder filter 1") {
        loFilterQty.filter(loRaw)
      }
      measureTime("lineorder filter 2") {
        loFilterDiscount1.filter(loFilterQty)
      }
      measureTime("lineorder filter 3") {
        loFilterDiscount2.filter(loFilterDiscount1)
      }
      measureTime("date join lineorder") {
        joinPartition.join()
      }
      measureTime("projection") {
        projectedPartition.project()
      }
      measureTime("aggregation") {
        //        aggregatePartition.aggregate()
        1
      }

      println(f"loFilterQty.size = ${loFilterQty.size}")
      println(f"loFilterDiscount1.size = ${loFilterDiscount1.size}")
      println(f"loFilterDiscount2.size = ${loFilterDiscount2.size}")
      println(f"joinPartition.size = ${joinPartition.size}")
      println(f"aggregatePartition.size= ${aggregatePartition.size}")

      //    (1 to ITERATIONS).map { iterationCount =>
      //
      //      val startComputationTimeStamp = System.nanoTime()
      //      aggregatePartition.foreachPartition {
      //        p => Unit
      //      }
      //      val endComputationTimeStamp = System.nanoTime()
      //
      //      val execTime = endComputationTimeStamp - startComputationTimeStamp
      //      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_1_1,${execTime},NA,${SCALE_FACTOR},SparkGPU")
      //
      //    }


      val endTime = System.nanoTime()
      val totalTime = endTime - startTime
      println("EXPERIMENT: total time = %,15d".format(totalTime))
      println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
      println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
      println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
      println("EXPERIMENT: ===================================")
      openCLContext.resetMetrics
    }
  }

  ignore("SSB_q1_2 test") {


    //    select sum(lo_extendedprice*lo_discount) as revenue
    //    from lineorder,ddate
    //    where lo_orderdate = d_datekey
    //    and d_yearmonth = '199401'
    //    and lo_discount>=4
    //    and lo_discount<=6
    //    and lo_quantity>=26
    //    and lo_quantity<=35

    val startTime = System.nanoTime

    /*__________________________ d_datekey. d_yearmonth _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE5")

    val dDateRaw = new GpuPartition[(Int,Int)](openCLContext, 3000)

    measureTime("date load") {
      dDateRaw.fillFromFiles(ddatePaths)
    }

    val filteredDatesPartition = new GpuFilteredPartition[(Int, Int), Int](openCLContext, 1, 1
      , ComparisonOperation.==, 199401, 3000)

    measureTime("date filter") {
      filteredDatesPartition.filter(dDateRaw)
    }

    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    /*____________________________ lo_extprice,dscnt,date, qty________________*/
    val loRaw = new GpuPartition[(Int, Int, Int, Int)](openCLContext, DEFAULT_CAPACITY)

    measureTime("lineorder load") {
      loRaw.fillFromFiles(lineOrderPaths)
    }

    val loFilterQty1 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext, 19, 3,
      ComparisonOperation.<=, 35, 1 << 26)

    measureTime("loFilterQty1 filter") {
      loFilterQty1.filter(loRaw)
    }

    val loFilterQty2 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext, 13, 3,
      ComparisonOperation.>=, 26, 1 << 24)

    measureTime("loFilterQty2 filter") {
      loFilterQty2.filter(loFilterQty1)

    }

    val loFilterDiscount1 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      14, 1, ComparisonOperation.<=, 6, 1 << 23)

    measureTime("loFilterDiscount1 filter") {
      loFilterDiscount1.filter(loFilterQty2)
    }

    val loFilterDiscount2 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      15, 1, ComparisonOperation.>=, 4, 1 << 22)

    measureTime("loFilterDiscount2 filter") {
      loFilterDiscount2.filter(loFilterDiscount1)
    }

    val joinPartition = new GpuJoinPartition[(Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int),
      Int](openCLContext, loFilterDiscount2, filteredDatesPartition, 2, 0, 1 << 16)

    measureTime("join"){
      joinPartition.join()
    }

    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
    val revenueAggregation = new AggregationExp(AggregationOperation.sum, revenueExp)

    val aggregatePartition = new GpuAggregationPartition[(Float, Float), (Int, Int, Int, Int, Int)](
      openCLContext, joinPartition, Array(constColumnGroupBy, revenueAggregation), 1 << 3)

    measureTime("aggregation"){
      aggregatePartition.aggregate()
    }

    val endTime = System.nanoTime()
    val totalTime = endTime - startTime
    println("EXPERIMENT: total time = %,15d".format(totalTime))
    println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    println("EXPERIMENT: ===================================")
    openCLContext.resetMetrics
  }

  ignore("SSB_q1_3 test") {

    //    select sum(lo_extendedprice*lo_discount) as revenue
    //    from lineorder,ddate
    //    where lo_orderdate = d_datekey
    //    and d_weeknuminyear = 6
    //    and d_year = 1994
    //    and lo_discount>=5
    //    and lo_discount<=7
    //    and lo_quantity>=26
    //    and lo_quantity<=35

    val startTime = System.nanoTime

    /*__________________________ d_datekey. d_year, d_weeknuminyear _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE11")

    val dDateRaw = new GpuPartition[(Int,Int, Int)](openCLContext, 3000)

    measureTime("date load") {
      dDateRaw.fillFromFiles(ddatePaths)
    }

    val filteredDatesPartition = new GpuFilteredPartition[(Int, Int, Int), Int](openCLContext
      , 101, 1, ComparisonOperation.==, 1994, 3000)

    measureTime("date filter 1") {
      filteredDatesPartition.filter(dDateRaw)
    }

    val filteredYearDatesPartition = new GpuFilteredPartition[(Int, Int, Int), Int](openCLContext,
      102, 2, ComparisonOperation.==, 6, 8)

    measureTime("date filter 2") {
      filteredYearDatesPartition.filter(filteredDatesPartition)
    }

    /*____________________________________price,dscnt,date, qty_________________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    val loRaw = new GpuPartition[(Int, Int, Int, Int)](openCLContext, DEFAULT_CAPACITY)

    measureTime("lineorder load") {
      loRaw.fillFromFiles(lineOrderPaths)
    }

    val loFilterQty1 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      201, 3, ComparisonOperation.>=, 26, 1 << 25)

    measureTime("loFilterQty1 filter") {
      loFilterQty1.filter(loRaw)
    }

    val loFilterQty2 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      202, 3, ComparisonOperation.<=, 36, 1 << 24)

    measureTime("loFilterQty2 filter") {
      loFilterQty2.filter(loFilterQty1)
    }

    val loFilterDiscount1 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      203, 1, ComparisonOperation.<=, 7, 1 << 24)

    measureTime("loFilterDiscount1 filter") {
      loFilterDiscount1.filter(loFilterQty2)
    }

    val loFilterDiscount2 = new GpuFilteredPartition[(Int, Int, Int, Int), Int](openCLContext,
      201, 1, ComparisonOperation.>=, 5, 1 << 23)

    measureTime("loFilterDiscount2 filter") {
      loFilterDiscount2.filter(loFilterDiscount1)
    }

    val joinPartition = new GpuJoinPartition[
      (Int, Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int, Int), Int](openCLContext,
      loFilterDiscount2, filteredYearDatesPartition, 2, 0, 1 << 14)

    measureTime("join"){
      joinPartition.join()
    }

    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
    val revenueAggregation = new AggregationExp(AggregationOperation.sum, revenueExp)

    val aggregatePartition = new GpuAggregationPartition[(Float, Float), (Int, Int, Int, Int,
      Int, Int)](openCLContext,
      joinPartition, Array(constColumnGroupBy, revenueAggregation), DEFAULT_CAPACITY)

    measureTime("aggregation"){
      aggregatePartition.aggregate()
    }

    val endTime = System.nanoTime()
    val totalTime = endTime - startTime
    println("EXPERIMENT: total time = %,15d".format(totalTime))
    println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    println("EXPERIMENT: ===================================")
    openCLContext.resetMetrics

  }

  test("SSB_q2_1 test") {

    //  select sum(lo_revenue),d_year,p_brand1
    //  from lineorder,ddate,part,supplier
    //  where lo_orderdate = d_datekey
    //  and lo_partkey = p_partkey
    //  and lo_suppkey = s_suppkey
    //  and p_category = 'MFGR#12'
    //  and s_region = 'AMERICA'
    //  group by d_year,p_brand1
    //  order by d_year,p_brand1

    val startTime = System.nanoTime

    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    val dDateRaw = new GpuPartition[(Int, Int)](openCLContext, 3000)

    measureTime("date load") {
      dDateRaw.fillFromFiles(ddatePaths)
    }

    /*____________________________________ supkey, s_region ________________________*/
    val supplierPaths = Array(
//      "/home/fathi/workspace/gpudb/database/SSB/scale-0.01/SUPPLIER0",
//      "/home/fathi/workspace/gpudb/database/SSB/scale-0.01/SUPPLIER5")
       "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    val supplierRaw = new GpuPartition[(Int, String)](openCLContext, 1 << 15)

    measureTime("supplier load") {
      supplierRaw.fillFromFiles(supplierPaths)
    }
    val t: ByteBuffer = supplierRaw.stringData(0)

    implicit def toCharIterator(byteBuffer: ByteBuffer): Iterator[Char] = {

      val iterator = new Iterator[Char](){

        val bb = byteBuffer
        var location = -1

        override def hasNext: Boolean = (location + 1)  < byteBuffer.limit

        override def next(): Char = {
          location += 1
          byteBuffer.get(location).toChar
        }
      }

      iterator
    }

    logInfo(f"Raw supplier regions ${t.mkString}")

//    (0 until supplierRaw.size).foreach{ i =>
//      if (i % 100 >= 0) println(f"supplierRaw($i)=${supplierRaw(i)}")
//    }

    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    val loRaw = new GpuPartition[(Int, Int, Int, Int)](openCLContext, DEFAULT_CAPACITY)

    measureTime("lineorder load") {
      loRaw.fillFromFiles(lineOrderPaths)
    }

    /*__________________________ key. brand, category _____________________________*/
    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART3")

    val partsRaw = new GpuPartition[(Int, String, String)](openCLContext, 1 << 20)

    measureTime("parts load") {
      partsRaw.fillFromFiles(partPaths)
    }

    (0 until 50).foreach{ i=> println(f"partsRaw($i)=${partsRaw(i)}")}

    val supplierFilter = new GpuFilteredPartition[(Int, String), String](openCLContext,
      21001, 1, ComparisonOperation.==, "AMERICA", 1 << 13)

    measureTime("supplier filter") {
      supplierFilter.filter(supplierRaw)
    }

    /*____________________________________ supkey ________________________*/
    val supplierProjection = new GpuProjectionPartition[Tuple1[Int], (Int, String)](
    openCLContext, supplierFilter, Array(0), 1 << 13)

    measureTime("supplier projection") {
      supplierProjection.project()
    }

    (0 until supplierProjection.size).foreach{ i =>
      if (i % 100 == 0) println(f"supplierProjection($i)=${supplierProjection(i)}")
    }

    /*______________________________ revenue,date,partkey,supkey, d_year____________________*/
    val loDateJoinPartition = new GpuJoinPartition[
      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](openCLContext,
        loRaw, dDateRaw, 1, 0, DEFAULT_CAPACITY)

    measureTime("join on date") {
      loDateJoinPartition.join()
    }

    /*______________________________ revenue,partkey,supkey, d_year____________________*/
    val loDateProjection = new GpuProjectionPartition[(Int, Int, Int, Int),
      (Int, Int, Int, Int, Int)](
        openCLContext, loDateJoinPartition, Array(0, 2, 3, 4), DEFAULT_CAPACITY)

    measureTime("projection after join with date"){
      loDateProjection.project()
    }

   /*__________________________ key. brand, category _____________________________*/
    val partsFilter = new GpuFilteredPartition[(Int, String, String), String](openCLContext,
      21005, 2, ComparisonOperation.==, "MFGR#12", 1 << 15)

    measureTime("parts filter") {
      partsFilter.filter(partsRaw)
    }

    /*__________________________ key. brand _____________________________*/
    val partsProjection = new GpuProjectionPartition [(Int, String), (Int, String, String)](
    openCLContext, partsFilter, Array(0, 1), 1 << 15)

    measureTime("parts projection") {
      partsProjection.project()
    }

    (0 until 50).foreach{ i=> println(f"partsProjection($i)=${partsProjection(i)}")}

    /*_______________________ revenue,partkey,supkey, d_year, brand __________________*/
    val loPartJoin = new GpuJoinPartition[
      (Int, Int, Int, Int, String), (Int, Int, Int, Int), (Int, String), Int](openCLContext,
        loDateProjection, partsProjection, 1, 0, 1 << 22)

    measureTime("join on parts") {
      loPartJoin.join()
    }

    (0 until 10).foreach{ i => println(f"loPartJoin($i)=${loPartJoin(i)}") }

    /*_______________________ revenue,supkey, d_year, brand __________________*/
    val loPartProjection = new GpuProjectionPartition[(Int, Int, Int, String),
      (Int, Int, Int, Int, String)](
        openCLContext, loPartJoin, Array(0, 2, 3, 4), 1 << 22)

    measureTime("projection after join with parts"){
      loPartProjection.project()
    }

    (0 until 10).foreach{ i => println(f"loPartProjection($i)=${loPartProjection(i)}") }

    /*_______________________ revenue,supkey, d_year, brand __________________*/
    val loSupplierJoin = new GpuJoinPartition[(Int, Int, Int, String),
      (Int, Int, Int, String), Tuple1[Int], Int](
        openCLContext, loPartProjection, supplierProjection, 1, 0, 1 << 19)

    measureTime("join on supplier") {
      loSupplierJoin.join()
    }


    /*_______________________ revenue, d_year, brand __________________*/
    val loSupplierProjection = new GpuProjectionPartition[(Int, Int, String),
      (Int, Int, Int, String)](
      openCLContext, loSupplierJoin, Array(0, 2, 3), 1 << 19)

    measureTime("projection after join with supplier"){
      loSupplierProjection.project()
    }

   (0 until 10).foreach{ i => println(f"loSupplierProjection($i)=${loSupplierProjection(i)}") }

    val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 2)
    val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)

    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)

    val aggregatePartition = new GpuAggregationPartition[(Float, Int, String),
      (Int, Int, String)](openCLContext,
        loSupplierProjection, Array(revenueSum, yearGB, brandGB), 1 << 15)

    measureTime("aggregation") {
      aggregatePartition.aggregate()
    }

//    val endTime = System.nanoTime()
//    val totalTime = endTime - startTime
//    println("EXPERIMENT: total time = %,15d".format(totalTime))
//    println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
//    println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
//    println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
//    println("EXPERIMENT: ===================================")
//    openCLContext.resetMetrics

  }

  ignore("SSB_q2_2 test") {

    //    select sum(lo_revenue),d_year,p_brand1
    //    from lineorder,ddate,part,supplier
    //    where lo_orderdate = d_datekey
    //    and lo_partkey = p_partkey
    //    and lo_suppkey = s_suppkey
    //    and p_brand1 >= 'MFGR#2221'
    //    and p_brand1 <= 'MFGR#2228'
    //    and s_region = 'ASIA'
    //    group by d_year,p_brand1
    //    order by d_year,p_brand1


//    /*__________________________ d_datekey. d_year _____________________________*/
//
//    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
//      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
//    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)
//
//
//    dDateRaw.cache()
//    dDateRaw.count()
//
//    /*____________________________________ supkey, s_region ________________________*/
//    val supplierPaths = Array(
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
//    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
//      DEFAULT_CAPACITY, 1)
//
//    supplierRaw.cache()
//    supplierRaw.count()
//
//    /*____________________________________ revenue,date,partkey,supkey ________________________*/
//    val lineOrderPaths = Array(
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
//      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
//    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY)
//
//    loRaw.cache()
//    loRaw.count()
//
//    /*__________________________ key. brand, category _____________________________*/
//    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
//      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4")
//
//    val partsRaw = sc.fromColumnarFiles[(Int, String)](partPaths, DEFAULT_CAPACITY)
//
//    partsRaw.cache()
//    partsRaw.count()
//
//
//    val loDateJoinPartition = new GpuJoinPartition[
//      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
//        loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY)
//
//    val supplierFilter = new GpuFilteredPartition[(Int, String), String](
//      supplierRaw, 1, ComparisonOperation.==, "AMERICA", DEFAULT_CAPACITY)
//
//
//    val loSupplierJoin = new GpuJoinPartition[
//      (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
//        loDateJoinPartition, supplierFilter, 3, 0, DEFAULT_CAPACITY)
//
//    val partsFilter1 = new GpuFilteredPartition[(Int, String), String](
//      partsRaw, 2, ComparisonOperation.>=, "MFGR#2221", DEFAULT_CAPACITY)
//
//    val partsFilter2 = new GpuFilteredPartition[(Int, String), String](
//      partsRaw, 2, ComparisonOperation.<=, "MFGR#2228", DEFAULT_CAPACITY)
//
//
//    val loPartJoin = new GpuJoinPartition[
//      (Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, String),
//      (Int, String), Int](
//        loSupplierJoin, partsFilter2, 2, 0, DEFAULT_CAPACITY)
//
//    val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
//    val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
//    val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
//
//    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
//    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
//    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)
//
//    val aggregatePartition = new GpuAggregationPartition[(Float, Int, String),
//      (Int, Int, Int, Int, Int, String, String)](
//        loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY)
//
//    (1 to ITERATIONS).map { iterationCount =>
//
//      val startComputationTimeStamp = System.nanoTime()
//      aggregatePartition.foreachPartition {
//        p => Unit
//      }
//      val endComputationTimeStamp = System.nanoTime()
//
//      val execTime = endComputationTimeStamp - startComputationTimeStamp
//      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_2,${execTime},NA,${SCALE_FACTOR},SparkGPU")
//    }

  }

  ignore("SSB_q2_3 test") {

    //    select sum(lo_revenue),d_year,p_brand1
    //    from lineorder,ddate,part,supplier
    //    where lo_orderdate = d_datekey
    //    and lo_partkey = p_partkey
    //    and lo_suppkey = s_suppkey
    //    and p_brand1 = 'MFGR#2239'
    //    and s_region = 'EUROPE'
    //    group by d_year,p_brand1
    //    order by d_year,p_brand1

    //
    //    /*__________________________ d_datekey. d_year _____________________________*/
    //
    //    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
    //      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    //    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)
    //
    //
    //    dDateRaw.cache()
    //    dDateRaw.count()
    //
    //    /*____________________________________ supkey, s_region ________________________*/
    //    val supplierPaths = Array(
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    //    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
    //      DEFAULT_CAPACITY, 1)
    //
    //    supplierRaw.cache()
    //    supplierRaw.count()
    //
    //    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    //    val lineOrderPaths = Array(
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    //    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY)
    //
    //    loRaw.cache()
    //    loRaw.count()
    //
    //    /*__________________________ key. brand, category _____________________________*/
    //    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
    //      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4")
    //
    //    val partsRaw = sc.fromColumnarFiles[(Int, String)](partPaths, DEFAULT_CAPACITY)
    //
    //    partsRaw.cache()
    //    partsRaw.count()
    //
    //    (1 to ITERATIONS).map { iterationCount =>
    //
    //      val loDateJoinPartition = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
    //          loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY)
    //
    //      val supplierFilter = new GpuFilteredPartition[(Int, String), String](
    //        supplierRaw, 1, ComparisonOperation.==, "EUROPE", DEFAULT_CAPACITY)
    //
    //
    //      val loSupplierJoin = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
    //          loDateJoinPartition, supplierFilter, 3, 0, DEFAULT_CAPACITY)
    //
    //      val partsFilter = new GpuFilteredPartition[(Int, String), String](
    //        partsRaw, 2, ComparisonOperation.==, "MFGR#2239", DEFAULT_CAPACITY)
    //
    //      val loPartJoin = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, String),
    //        (Int, String), Int](
    //          loSupplierJoin, partsFilter, 2, 0, DEFAULT_CAPACITY)
    //
    //      val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
    //      val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
    //      val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    //
    //      val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    //      val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    //      val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)
    //
    //      val aggregatePartition = new GpuAggregationPartition[(Float, Int, String),
    //        (Int, Int, Int, Int, Int, String, String)](
    //          loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY)
    //
    //
    //      val startComputationTimeStamp = System.nanoTime()
    //      aggregatePartition.foreachPartition {
    //        p => Unit
    //      }
    //      val endComputationTimeStamp = System.nanoTime()
    //
    //      val execTime = endComputationTimeStamp - startComputationTimeStamp
    //      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_3,${execTime},NA,${SCALE_FACTOR},SparkGPU")
    //      if (iterationCount == 1) {
    //
    //
    //        println(f"[RESULTS]: loDateJoinPartition.count = ${loDateJoinPartition.count}")
    //        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
    //        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
    //        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
    //        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
    //        println(f"[RESULTS]: aggregatePartition.count = ${aggregatePartition.count}")
    //
    //        println(f"[RESULTS]: loDateJoinPartition.count = ${loDateJoinPartition.count}")
    //        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
    //        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
    //        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
    //        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
    //        println(f"[RESULTS]: aggregatePartition.count = ${aggregatePartition.count}")
    //
    //        println(f"lo join date results ${flattenResults(loDateJoinPartition.collect()).size}")
    //        flattenResults(loDateJoinPartition.collect()).foreach(r => println(f"[RESULTS]: loDateJoinPartition${r}"))
    //        flattenResults(supplierFilter.collect()).foreach(r => println(f"[RESULTS]: supplierFilte${r}"))
    //        flattenResults(loSupplierJoin.collect()).foreach(r => println(f"[RESULTS]: loSupplierJoi${r}"))
    //        flattenResults(partsFilter.collect()).foreach(r => println(f"[RESULTS]: partsFilter.c${r}"))
    //        flattenResults(loPartJoin.collect()).foreach(r => println(f"[RESULTS]: loPartJoin.co${r}"))
    //        flattenResults(aggregatePartition.collect()).foreach(r => println(f"[RESULTS]: aggregatePartition.${r}"))
    //      }
    //
    //      println("[DEBUGGING]: Unpersisting all intermediate RDDS ...")
    //      loDateJoinPartition.unpersist(true)
    //      supplierFilter.unpersist(true)
    //      loSupplierJoin.unpersist(true)
    //      partsFilter.unpersist(true)
    //      loPartJoin.unpersist(true)
    //      aggregatePartition.unpersist(true)
    //      println("[DEBUGGING]: ... finished unpersisting all intermediate RDDS")
    //
    //    }
  }

  ignore("SSB_q3_1 test") {

//    select c_nation,s_nation,d_year,sum(lo_revenue) as revenue
//    from customer,lineorder,supplier,ddate
//    where lo_custkey = c_custkey
//    and lo_suppkey = s_suppkey
//    and lo_orderdate = d_datekey
//    and c_region = 'ASIA'
//    and s_region = 'ASIA'
//    and d_year >=1992 and d_year <= 1997
//    group by c_nation,s_nation,d_year
//    order by d_year asc,revenue desc;

    //
    //    /*__________________________ d_datekey. d_year _____________________________*/
    //
    //    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
    //      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    //    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)
    //
    //
    //    dDateRaw.cache()
    //    dDateRaw.count()
    //
    //    /*____________________________________ supkey, s_region ________________________*/
    //    val supplierPaths = Array(
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    //    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
    //      DEFAULT_CAPACITY, 1)
    //
    //    supplierRaw.cache()
    //    supplierRaw.count()
    //
    //    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    //    val lineOrderPaths = Array(
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
    //      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    //    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY)
    //
    //    loRaw.cache()
    //    loRaw.count()
    //
    //    /*__________________________ key. brand, category _____________________________*/
    //    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
    //      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4")
    //
    //    val partsRaw = sc.fromColumnarFiles[(Int, String)](partPaths, DEFAULT_CAPACITY)
    //
    //    partsRaw.cache()
    //    partsRaw.count()
    //
    //    (1 to ITERATIONS).map { iterationCount =>
    //
    //      val loDateJoinPartition = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
    //          loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY)
    //
    //      val supplierFilter = new GpuFilteredPartition[(Int, String), String](
    //        supplierRaw, 1, ComparisonOperation.==, "EUROPE", DEFAULT_CAPACITY)
    //
    //
    //      val loSupplierJoin = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
    //          loDateJoinPartition, supplierFilter, 3, 0, DEFAULT_CAPACITY)
    //
    //      val partsFilter = new GpuFilteredPartition[(Int, String), String](
    //        partsRaw, 2, ComparisonOperation.==, "MFGR#2239", DEFAULT_CAPACITY)
    //
    //      val loPartJoin = new GpuJoinPartition[
    //        (Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, String),
    //        (Int, String), Int](
    //          loSupplierJoin, partsFilter, 2, 0, DEFAULT_CAPACITY)
    //
    //      val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
    //      val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
    //      val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    //
    //      val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    //      val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    //      val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)
    //
    //      val aggregatePartition = new GpuAggregationPartition[(Float, Int, String),
    //        (Int, Int, Int, Int, Int, String, String)](
    //          loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY)
    //
    //
    //      val startComputationTimeStamp = System.nanoTime()
    //      aggregatePartition.foreachPartition {
    //        p => Unit
    //      }
    //      val endComputationTimeStamp = System.nanoTime()
    //
    //      val execTime = endComputationTimeStamp - startComputationTimeStamp
    //      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_3,${execTime},NA,${SCALE_FACTOR},SparkGPU")
    //      if (iterationCount == 1) {
    //
    //
    //        println(f"[RESULTS]: loDateJoinPartition.count = ${loDateJoinPartition.count}")
    //        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
    //        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
    //        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
    //        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
    //        println(f"[RESULTS]: aggregatePartition.count = ${aggregatePartition.count}")
    //
    //        println(f"[RESULTS]: loDateJoinPartition.count = ${loDateJoinPartition.count}")
    //        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
    //        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
    //        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
    //        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
    //        println(f"[RESULTS]: aggregatePartition.count = ${aggregatePartition.count}")
    //
    //        println(f"lo join date results ${flattenResults(loDateJoinPartition.collect()).size}")
    //        flattenResults(loDateJoinPartition.collect()).foreach(r => println(f"[RESULTS]: loDateJoinPartition${r}"))
    //        flattenResults(supplierFilter.collect()).foreach(r => println(f"[RESULTS]: supplierFilte${r}"))
    //        flattenResults(loSupplierJoin.collect()).foreach(r => println(f"[RESULTS]: loSupplierJoi${r}"))
    //        flattenResults(partsFilter.collect()).foreach(r => println(f"[RESULTS]: partsFilter.c${r}"))
    //        flattenResults(loPartJoin.collect()).foreach(r => println(f"[RESULTS]: loPartJoin.co${r}"))
    //        flattenResults(aggregatePartition.collect()).foreach(r => println(f"[RESULTS]: aggregatePartition.${r}"))
    //      }
    //
    //      println("[DEBUGGING]: Unpersisting all intermediate RDDS ...")
    //      loDateJoinPartition.unpersist(true)
    //      supplierFilter.unpersist(true)
    //      loSupplierJoin.unpersist(true)
    //      partsFilter.unpersist(true)
    //      loPartJoin.unpersist(true)
    //      aggregatePartition.unpersist(true)
    //      println("[DEBUGGING]: ... finished unpersisting all intermediate RDDS")
    //
    //    }
  }

}
