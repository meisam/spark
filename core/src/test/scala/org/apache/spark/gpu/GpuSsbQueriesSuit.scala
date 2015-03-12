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

import org.apache.spark.rdd._
import org.apache.spark.scheduler.OpenCLContext

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
class GpuSsbQueriesSuit extends GpuSuit {


  override val DEFAULT_CAPACITY = 1 << 27

  val ITERATIONS = 20

  test("SSB_q1_1 test") {
    val startTime = System.nanoTime()
    val openCLContext = new OpenCLContext
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")

    // select sum(lo_extendedprice*lo_discount) as revenue
    // from lineorder, ddate
    // where lo_orderdate = d_datekey
    // and d_year = 1993
    // and lo_discount>=1
    // and lo_discount<=3
    // and lo_quantity<25;

    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")

    val dDateRaw: GpuRDD[(Int, Int)] = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)

    dDateRaw.cache()

    val filerYearEnd = System.nanoTime()
    dDateRaw.count()
    val filerYearStart = System.nanoTime()

    val filterYearTime = filerYearEnd - filerYearStart

    val filteredDatesRdd: GpuFilteredRDD[(Int, Int), Int] = new GpuFilteredRDD[(Int, Int), Int](dDateRaw, 1
      , ComparisonOperation.==, 1993, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    //            /*____________________________________price,dscnt,date, qty_________________________________*/
    val loRaw = sc.fromColumnarFiles[(Float, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, 1)

    loRaw.count()
    val lineOrderReadStartTime = System.nanoTime()
    loRaw.cache()
    val lineOrderReadEndTime = System.nanoTime()
    val lineOrderReadTime = lineOrderReadEndTime - lineOrderReadStartTime

    val filterLineorderStart = System.nanoTime()

    val loFilterQty = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loRaw, 3,
      ComparisonOperation.<, 25, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty, 1, ComparisonOperation.<=, 3, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterDiscount1,
      1, ComparisonOperation.>=, 1, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val joinRdd = new GpuJoinRDD[(Float, Int, Int, Int, Int), (Float, Int, Int, Int), (Int, Int),
      Int](loFilterDiscount2, filteredDatesRdd, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val priceAggregation = new AggregationExp(AggregationOperation.sum, col0)
    val discountAggregation = new AggregationExp(AggregationOperation.sum, col1)

    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Float, Int, Int, Int, Int)](
      joinRdd, Array(priceAggregation, discountAggregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val times = (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      val results = aggregateRdd.collect()
      val endComputationTimeStamp = System.nanoTime()

      endComputationTimeStamp - startComputationTimeStamp
    }

    val averageTime = times.sum / times.length
    println(f"Average time to compute SSB 1.1 is ${averageTime}%,d nano seconds capacity = ${DEFAULT_CAPACITY}")

    println(times.mkString("exec times= [", ",", "]"))

    //
    //            val endTime = System.nanoTime()
    //            val totaltime = endTime - startTime;
    //            println("EXPERIMENT: total time = %,15d".format(totaltime))
    //            println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    //            println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    //            println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    //            println("EXPERIMENT: filterYearTime = %,15d".format(filterYearTime))
    //            println("EXPERIMENT: filterLineorderTime = %,15d".format(filterLineorderTime))
    //            println("EXPERIMENT: joinTime= %,15d".format(joinTime))
    //            println("EXPERIMENT: aggregatioTime= %,15d".format(aggregatioTime))
    //            println("EXPERIMENT: ===================================")

  }


  ignore("SSB_q1_2 test") {

    val startTime = System.nanoTime()
    val openCLContext = new OpenCLContext
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")

    //    select sum(lo_extendedprice*lo_discount) as revenue
    //    from lineorder,ddate
    //    where lo_orderdate = d_datekey
    //    and d_yearmonth = '199401'
    //    and lo_discount>=4
    //    and lo_discount<=6
    //    and lo_quantity>=26
    //    and lo_quantity<=35;

    /*__________________________ d_datekey. d_yearmonth _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE5")

    val dDateRaw: GpuRDD[(Int, Int)] = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)

    dDateRaw.cache()

    dDateRaw.count()

    val filerYearEnd = System.nanoTime()

    val filerYearStart = System.nanoTime()

    val filterYearTime = filerYearEnd - filerYearStart

    val filteredDatesRdd: GpuFilteredRDD[(Int, Int), Int] = new GpuFilteredRDD[(Int, Int), Int](dDateRaw, 1
      , ComparisonOperation.==, 199401, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    /*____________________________ lo_extprice,dscnt,date, qty________________*/
    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, 1)

    loRaw.cache()
    loRaw.count()

    val filterLineorderStart = System.nanoTime()

    val loFilterQty1 = new GpuFilteredRDD[(Int, Int, Int, Int), Int](loRaw, 3,
      ComparisonOperation.<=, 35, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)
    //
    val loFilterQty2 = new GpuFilteredRDD[(Int, Int, Int, Int), Int](loFilterQty1, 3,
      ComparisonOperation.>=, 26, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount1 = new GpuFilteredRDD[(Int, Int, Int, Int), Int](loFilterQty2, 1,
      ComparisonOperation.<=, 6, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount2 = new GpuFilteredRDD[(Int, Int, Int, Int), Int](loFilterDiscount1, 1,
      ComparisonOperation.>=, 4, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val joinRdd = new GpuJoinRDD[(Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int),
      Int](loFilterDiscount2, filteredDatesRdd, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val priceAggregation = new AggregationExp(AggregationOperation.sum, col0)
    val discountAggregation = new AggregationExp(AggregationOperation.sum, col1)

    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Int, Int, Int, Int, Int)](
      joinRdd, Array(priceAggregation, discountAggregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val times = (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      val results = aggregateRdd.collect()
      val endComputationTimeStamp = System.nanoTime()

      endComputationTimeStamp - startComputationTimeStamp
    }

    val averageTime = times.sum / times.length
    println(f"Average time to compute SSB 1.2 is ${averageTime}%,d nano seconds capacity = ${DEFAULT_CAPACITY}")

    println(times.mkString("exec times= [", ",", "]"))

    //
    //            val endTime = System.nanoTime()
    //            val totaltime = endTime - startTime;
    //            println("EXPERIMENT: total time = %,15d".format(totaltime))
    //            println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    //            println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    //            println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    //            println("EXPERIMENT: filterYearTime = %,15d".format(filterYearTime))
    //            println("EXPERIMENT: filterLineorderTime = %,15d".format(filterLineorderTime))
    //            println("EXPERIMENT: joinTime= %,15d".format(joinTime))
    //            println("EXPERIMENT: aggregatioTime= %,15d".format(aggregatioTime))
    //            println("EXPERIMENT: ===================================")

  }

  ignore("SSB_q1_3 test") {

    val startTime = System.nanoTime()
    val openCLContext = new OpenCLContext
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")

    //    select sum(lo_extendedprice*lo_discount) as revenue
    //    from lineorder,ddate
    //    where lo_orderdate = d_datekey
    //    and d_weeknuminyear = 6
    //    and d_year = 1994
    //    and lo_discount>=5
    //    and lo_discount<=7
    //    and lo_quantity>=26
    //    and lo_quantity<=35;

    /*__________________________ d_datekey. d_year, d_weeknuminyear _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE11")

    val dDateRaw = sc.fromColumnarFiles[(Int, Int, Int)](ddatePaths, 1000, 1)

    dDateRaw.cache()

    dDateRaw.count()

    val filerYearEnd = System.nanoTime()

    val filerYearStart = System.nanoTime()

    val filterYearTime = filerYearEnd - filerYearStart

    val filteredDatesRdd = new GpuFilteredRDD[(Int, Int, Int), Int](dDateRaw, 1
      , ComparisonOperation.==, 1994, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val filteredYearDatesRdd = new GpuFilteredRDD[(Int, Int, Int), Int](filteredDatesRdd, 2
      , ComparisonOperation.==, 6, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    //            /*____________________________________price,dscnt,date, qty_________________________________*/
    val loRaw = sc.fromColumnarFiles[(Float, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, 1)

    loRaw.cache()
    loRaw.count()

    val filterLineorderStart = System.nanoTime()

    val loFilterQty1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loRaw, 3,
      ComparisonOperation.>=, 26, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterQty2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty1, 3,
      ComparisonOperation.<=, 36, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty2, 1,
      ComparisonOperation.<=, 7, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterDiscount1, 1,
      ComparisonOperation.>=, 5, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val joinRdd = new GpuJoinRDD[
      (Float, Int, Int, Int, Int, Int), (Float, Int, Int, Int), (Int, Int, Int), Int](
        loFilterDiscount2, filteredYearDatesRdd, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val priceAggregation = new AggregationExp(AggregationOperation.sum, col0)
    val discountAggregation = new AggregationExp(AggregationOperation.sum, col1)

    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Float, Int, Int, Int, Int, Int)](
      joinRdd, Array(priceAggregation, discountAggregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val times = (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      val results = aggregateRdd.collect()
      val endComputationTimeStamp = System.nanoTime()

      endComputationTimeStamp - startComputationTimeStamp
    }

    val averageTime = times.sum / times.length
    println(f"Average time to compute SSB 1.3 is ${averageTime}%,d nano seconds capacity = ${DEFAULT_CAPACITY}")

    println(times.mkString("exec times= [", ",", "]"))

    //
    //            val endTime = System.nanoTime()
    //            val totaltime = endTime - startTime;
    //            println("EXPERIMENT: total time = %,15d".format(totaltime))
    //            println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    //            println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    //            println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    //            println("EXPERIMENT: filterYearTime = %,15d".format(filterYearTime))
    //            println("EXPERIMENT: filterLineorderTime = %,15d".format(filterLineorderTime))
    //            println("EXPERIMENT: joinTime= %,15d".format(joinTime))
    //            println("EXPERIMENT: aggregatioTime= %,15d".format(aggregatioTime))
    //            println("EXPERIMENT: ===================================")

  }

  ignore("SSB_q1_4 test") {

    val startTime = System.nanoTime()
    val openCLContext = new OpenCLContext
    openCLContext.initOpenCL("/org/apache/spark/gpu/kernel.cl")

    //  select sum(lo_revenue),d_year,p_brand1
    //  from lineorder,ddate,part,supplier
    //  where lo_orderdate = d_datekey
    //  and lo_partkey = p_partkey
    //  and lo_suppkey = s_suppkey
    //  and p_category = 'MFGR#12'
    //  and s_region = 'AMERICA'
    //  group by d_year,p_brand1
    //  order by d_year,p_brand1;

    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)


    dDateRaw.cache()

    dDateRaw.count()

    val filerYearEnd = System.nanoTime()

    val filerYearStart = System.nanoTime()

    val filterYearTime = filerYearEnd - filerYearStart

    /*__________________________ key. brand, category _____________________________*/
    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART3")

    val partsRaw = sc.fromColumnarFiles[(Int, String, String)](partPaths, DEFAULT_CAPACITY, 1)

    partsRaw.cache()
    partsRaw.count()

    /*____________________________________revenue,date,partkey,supkey________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, 1)

    loRaw.cache()
    loRaw.count()

    val loDateJoinRdd = new GpuJoinRDD[
      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
        loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loPartJoin = new GpuJoinRDD[(Int, Int, Int, Int, Int, String, String), (Int, Int, Int,
      Int, Int), (Int, String, String), Int](loDateJoinRdd, partsRaw, 4, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val col0 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val col1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val yearColumn = new MathExp(MathOp.NOOP, 4, null, null, MathOperationType.column, 0)
    val brandColumn = new MathExp(MathOp.NOOP, 5, null, null, MathOperationType.column, 0)
    val revenueColumn = new MathExp(MathOp.NOOP, 5, null, null, MathOperationType.column, 0)
    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)

    val aggregateRdd = new GpuAggregationRDD[(Float, Int, String), (Int, Int, Int, Int, Int,
      String, String)](
        loPartJoin, Array(yearGB, brandGB, revenueSum), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val times = (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      val results = aggregateRdd.collect()
      val endComputationTimeStamp = System.nanoTime()

      endComputationTimeStamp - startComputationTimeStamp
    }

    val averageTime = times.sum / times.length
    println(f"Average time to compute SSB 1.4 is ${averageTime}%,d nano nano seconds capacity = ${DEFAULT_CAPACITY}")

    println(times.mkString("exec times= [", ",", "]"))

    //
    //            val endTime = System.nanoTime()
    //            val totaltime = endTime - startTime;
    //            println("EXPERIMENT: total time = %,15d".format(totaltime))
    //            println("EXPERIMENT: Disk Read = %,15d".format(openCLContext.diskReadTime))
    //            println("EXPERIMENT: PCI transfer time = %,15d".format(openCLContext.pciTransferTime))
    //            println("EXPERIMENT: PCI transfer byte = %,15d".format(openCLContext.pciTransferBytes))
    //            println("EXPERIMENT: filterYearTime = %,15d".format(filterYearTime))
    //            println("EXPERIMENT: filterLineorderTime = %,15d".format(filterLineorderTime))
    //            println("EXPERIMENT: joinTime= %,15d".format(joinTime))
                println("EXPERIMENT: pciTransferTime= %,15d".format(openCLContext.pciTransferTime))
    //            println("EXPERIMENT: ===================================")

  }
}
