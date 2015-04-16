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


  println(f"[PROFILING RESULTS]:Iteration,QueryName,ExecTime,ResultRowCount,ScaleFactor,Comments")

  override val DEFAULT_CAPACITY = 1 << 25

  val ITERATIONS = 5

  val SCALE_FACTOR = 10

  ignore("SSB_q1_1 test") {
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

    val dDateRaw: GpuRDD[(Int, Int)] = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, NUMBER_OF_PARTITIONS)

    dDateRaw.cache()
    dDateRaw.count()

    val filteredDatesRdd: GpuFilteredRDD[(Int, Int), Int] = new GpuFilteredRDD[(Int, Int), Int](dDateRaw, 1
      , ComparisonOperation.==, 1993, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    //            /*____________________________________price,dscnt,date, qty_________________________________*/
    val loRaw = sc.fromColumnarFiles[(Float, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, 1)

    loRaw.cache()
    loRaw.count()

    val loFilterQty = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loRaw, 3,
      ComparisonOperation.<, 25, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty, 1, ComparisonOperation.<=, 3, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterDiscount1,
      1, ComparisonOperation.>=, 1, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val joinRdd = new GpuJoinRDD[(Float, Int, Int, Int, Int), (Float, Int, Int, Int), (Int, Int),
      Int](loFilterDiscount2, filteredDatesRdd, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
    val revenueAgregation = new AggregationExp(AggregationOperation.sum, revenueExp)

    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Float, Int, Int, Int, Int)](
      joinRdd, Array(constColumnGroupBy, revenueAgregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      aggregateRdd.foreachPartition {
        p => Unit
      }
      val endComputationTimeStamp = System.nanoTime()

      val execTime = endComputationTimeStamp - startComputationTimeStamp
      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_1_1,${execTime},NA,${SCALE_FACTOR},SparkGPU")

    }

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


  test("SSB_q1_2 test") {


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

    val dateResults = flattenResults[(Int,Int)](dDateRaw.collect)
    println(f"[RESULTS]: dateResults.count=${dateResults.size}")

    val filteredDatesRdd: GpuFilteredRDD[(Int, Int), Int] = new GpuFilteredRDD[(Int, Int), Int](dDateRaw, 1
      , ComparisonOperation.==, 199401, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val dateFilterResults = flattenResults[(Int,Int)](filteredDatesRdd.collect)
    println(f"[RESULTS]: dateFilterResults.count=${dateFilterResults.size}")

    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER9",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER11",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER8")
    /*____________________________ lo_extprice,dscnt,date, qty________________*/
    val loRaw = sc.fromColumnarFiles[(Float, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    loRaw.cache()
    loRaw.count()

    val countingLoRdd: RDD[Int] = dDateRaw.mapPartitions({p => p.map(s => s.size)})

    val descriptiveStr = countingLoRdd.map{
      size =>
        f"partition.size = ${size}"
    }.collect

    println(f"dDateRaw size summarized = ${descriptiveStr.mkString(",")}")

//    val loResults = flattenResults[(Float, Int, Int, Int)](loRaw.collect)
//    println(f"[RESULTS]: loResults.count=${loResults.size}")

    val loFilterQty1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loRaw, 3,
      ComparisonOperation.<=, 35, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)
    //
//    val loFilterQty2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty1, 3,
//      ComparisonOperation.>=, 26, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loFilterDiscount1 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterQty1, 1,
      ComparisonOperation.<=, 6, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    println(f"[DEBIGGING]: loFilterDiscount1.str=${loFilterDiscount1.toDebugString}")
    val loFilterResults = flattenResults[(Float, Int, Int, Int)](loFilterDiscount1.collect)
    println(f"[RESULTS]: loFilterDiscount1.count=${loFilterResults.size}")

//    val loFilterDiscount2 = new GpuFilteredRDD[(Float, Int, Int, Int), Int](loFilterDiscount1, 1,
//      ComparisonOperation.>=, 4, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)
//
//
//    val joinRdd = new GpuJoinRDD[(Float, Int, Int, Int, Int), (Float, Int, Int, Int), (Int, Int),
//      Int](loFilterDiscount2, filteredDatesRdd, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)
//
//    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
//    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
//    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
//    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)
//
//
//    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
//    val revenueAggregation = new AggregationExp(AggregationOperation.sum, revenueExp)
//
//    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Float, Int, Int, Int, Int)](
//      joinRdd, Array(constColumnGroupBy, revenueAggregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)
//
//
//    (1 to ITERATIONS).map { iterationCount =>
//
//      val startComputationTimeStamp = System.nanoTime()
//      aggregateRdd.foreachPartition {
//        p => Unit
//      }
//      val endComputationTimeStamp = System.nanoTime()
//
//      val execTime = endComputationTimeStamp - startComputationTimeStamp
//      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_1_2,${execTime},NA,${SCALE_FACTOR},SparkGPU")
//    }

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

    val dDateRaw = sc.fromColumnarFiles[(Int, Int, Int)](ddatePaths, 1000, NUMBER_OF_PARTITIONS)

    dDateRaw.cache()

    dDateRaw.count()

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
    val loRaw = sc.fromColumnarFiles[(Float, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    loRaw.cache()
    loRaw.count()

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

    val extPriceColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)
    val discountColumnExpr = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 1)
    val revenueExp = new MathExp(MathOp.MULTIPLY, 2, extPriceColumnExpr, discountColumnExpr, MathOperationType.column, 1)
    val const1 = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.const, -1)


    val constColumnGroupBy = new AggregationExp(AggregationOperation.groupBy, const1)
    val revenueAggregation = new AggregationExp(AggregationOperation.sum, revenueExp)

    val aggregateRdd = new GpuAggregationRDD[(Float, Float), (Float, Int, Int, Int, Int, Int)](
      joinRdd, Array(constColumnGroupBy, revenueAggregation), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      aggregateRdd.foreachPartition {
        p => Unit
      }
      val endComputationTimeStamp = System.nanoTime()

      val execTime = endComputationTimeStamp - startComputationTimeStamp
      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_1_2,${execTime},NA,${SCALE_FACTOR},SparkGPU")
    }

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

  ignore("SSB_q2_1 test") {

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

    /*____________________________________ supkey, s_region ________________________*/
    val supplierPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
      DEFAULT_CAPACITY, 1)

    supplierRaw.cache()
    supplierRaw.count()

    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    loRaw.cache()
    loRaw.count()

    /*__________________________ key. brand, category _____________________________*/
    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART3")

    val partsRaw = sc.fromColumnarFiles[(Int, String, String)](partPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    partsRaw.cache()
    partsRaw.count()


    val loDateJoinRdd = new GpuJoinRDD[
      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
        loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val supplierFilter = new GpuFilteredRDD[(Int, String), String](
      supplierRaw, 1, ComparisonOperation.==, "AMERICA", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val loSupplierJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
        loDateJoinRdd, supplierFilter, 3, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val partsFilter = new GpuFilteredRDD[(Int, String, String), String](
      partsRaw, 2, ComparisonOperation.==, "MFGR#12", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val loPartJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String, String, String), (Int, Int, Int, Int, Int, String),
      (Int, String, String), Int](
        loSupplierJoin, partsFilter, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
    val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
    val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)

    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)

    val aggregateRdd = new GpuAggregationRDD[(Float, Int, String),
      (Int, Int, Int, Int, Int, String, String, String)](
        loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      aggregateRdd.foreachPartition {
        p => Unit
      }
      val endComputationTimeStamp = System.nanoTime()

      val execTime = endComputationTimeStamp - startComputationTimeStamp
      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_1,${execTime},NA,${SCALE_FACTOR},SparkGPU")
    }

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
    //    order by d_year,p_brand1;


    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)


    dDateRaw.cache()
    dDateRaw.count()

    /*____________________________________ supkey, s_region ________________________*/
    val supplierPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
      DEFAULT_CAPACITY, 1)

    supplierRaw.cache()
    supplierRaw.count()

    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    loRaw.cache()
    loRaw.count()

    /*__________________________ key. brand, category _____________________________*/
    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4")

    val partsRaw = sc.fromColumnarFiles[(Int, String)](partPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    partsRaw.cache()
    partsRaw.count()


    val loDateJoinRdd = new GpuJoinRDD[
      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
        loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val supplierFilter = new GpuFilteredRDD[(Int, String), String](
      supplierRaw, 1, ComparisonOperation.==, "AMERICA", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val loSupplierJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
        loDateJoinRdd, supplierFilter, 3, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val partsFilter1 = new GpuFilteredRDD[(Int, String), String](
      partsRaw, 2, ComparisonOperation.>=, "MFGR#2221", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val partsFilter2 = new GpuFilteredRDD[(Int, String), String](
      partsRaw, 2, ComparisonOperation.<=, "MFGR#2228", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val loPartJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, String),
      (Int, String), Int](
        loSupplierJoin, partsFilter2, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
    val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
    val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)

    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)

    val aggregateRdd = new GpuAggregationRDD[(Float, Int, String),
      (Int, Int, Int, Int, Int, String, String)](
        loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    (1 to ITERATIONS).map { iterationCount =>

      val startComputationTimeStamp = System.nanoTime()
      aggregateRdd.foreachPartition {
        p => Unit
      }
      val endComputationTimeStamp = System.nanoTime()

      val execTime = endComputationTimeStamp - startComputationTimeStamp
      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_2,${execTime},NA,${SCALE_FACTOR},SparkGPU")
    }

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
//    order by d_year,p_brand1;


    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 1000, 1)


    dDateRaw.cache()
    dDateRaw.count()

    /*____________________________________ supkey, s_region ________________________*/
    val supplierPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    val supplierRaw = sc.fromColumnarFiles[(Int, String)](supplierPaths,
      DEFAULT_CAPACITY, 1)

    supplierRaw.cache()
    supplierRaw.count()

    /*____________________________________ revenue,date,partkey,supkey ________________________*/
    val lineOrderPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER12",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER5",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER3",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/LINEORDER4")
    val loRaw = sc.fromColumnarFiles[(Int, Int, Int, Int)](lineOrderPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    loRaw.cache()
    loRaw.count()

    /*__________________________ key. brand, category _____________________________*/
    val partPaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/PART0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/PART4")

    val partsRaw = sc.fromColumnarFiles[(Int, String)](partPaths, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    partsRaw.cache()
    partsRaw.count()

    (1 to ITERATIONS).map { iterationCount =>

    val loDateJoinRdd = new GpuJoinRDD[
      (Int, Int, Int, Int, Int), (Int, Int, Int, Int), (Int, Int), Int](
        loRaw, dDateRaw, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val supplierFilter = new GpuFilteredRDD[(Int, String), String](
      supplierRaw, 1, ComparisonOperation.==, "EUROPE", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


    val loSupplierJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String), (Int, Int, Int, Int, Int), (Int, String), Int](
        loDateJoinRdd, supplierFilter, 3, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val partsFilter = new GpuFilteredRDD[(Int, String), String](
      partsRaw, 2, ComparisonOperation.==, "MFGR#2239", DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val loPartJoin = new GpuJoinRDD[
      (Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, String),
      (Int, String), Int](
        loSupplierJoin, partsFilter, 2, 0, DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)

    val yearColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 4)
    val brandColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 6)
    val revenueColumn = new MathExp(MathOp.NOOP, 1, null, null, MathOperationType.column, 0)

    val yearGB = new AggregationExp(AggregationOperation.groupBy, yearColumn)
    val brandGB = new AggregationExp(AggregationOperation.groupBy, brandColumn)
    val revenueSum = new AggregationExp(AggregationOperation.sum, revenueColumn)

    val aggregateRdd = new GpuAggregationRDD[(Float, Int, String),
      (Int, Int, Int, Int, Int, String, String)](
        loPartJoin, Array(revenueSum, yearGB, brandGB), DEFAULT_CAPACITY, NUMBER_OF_PARTITIONS)


      val startComputationTimeStamp = System.nanoTime()
      aggregateRdd.foreachPartition {
        p => Unit
      }
      val endComputationTimeStamp = System.nanoTime()

      val execTime = endComputationTimeStamp - startComputationTimeStamp
      println(f"[PROFILING RESULTS]: ${iterationCount},ssb_2_3,${execTime},NA,${SCALE_FACTOR},SparkGPU")
      if (iterationCount == 1) {


        println(f"[RESULTS]: loDateJoinRdd.count = ${loDateJoinRdd.count}")
        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
        println(f"[RESULTS]: aggregateRdd.count = ${aggregateRdd.count}")

        println(f"[RESULTS]: loDateJoinRdd.count = ${loDateJoinRdd.count}")
        println(f"[RESULTS]: supplierFilter.count = ${supplierFilter.count}")
        println(f"[RESULTS]: loSupplierJoin.count = ${loSupplierJoin.count}")
        println(f"[RESULTS]: partsFilter.count = ${partsFilter.count}")
        println(f"[RESULTS]: loPartJoin.count = ${loPartJoin.count}")
        println(f"[RESULTS]: aggregateRdd.count = ${aggregateRdd.count}")

        println(f"lo join date results ${flattenResults(loDateJoinRdd.collect()).size}")
        flattenResults(loDateJoinRdd.collect()).foreach(r => println(f"[RESULTS]: loDateJoinRdd${r}"))
        flattenResults(supplierFilter.collect()).foreach(r => println(f"[RESULTS]: supplierFilte${r}"))
        flattenResults(loSupplierJoin.collect()).foreach(r => println(f"[RESULTS]: loSupplierJoi${r}"))
        flattenResults(partsFilter.collect()).foreach(r => println(f"[RESULTS]: partsFilter.c${r}"))
        flattenResults(loPartJoin.collect()).foreach(r => println(f"[RESULTS]: loPartJoin.co${r}"))
        flattenResults(aggregateRdd.collect()).foreach(r => println(f"[RESULTS]: aggregateRdd.${r}"))
      }

      println("[DEBUGGING]: Unpersisting all intermediate RDDS ...")
      loDateJoinRdd.unpersist(true)
      supplierFilter.unpersist(true)
      loSupplierJoin.unpersist(true)
      partsFilter.unpersist(true)
      loPartJoin.unpersist(true)
      aggregateRdd.unpersist(true)
      println("[DEBUGGING]: ... finished unpersisting all intermediate RDDS")

    }
  }

  ignore("collect on large data test") {
    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE4")
    val dDateRaw = sc.fromColumnarFiles[(Int, Int)](ddatePaths, 639, 1)


    dDateRaw.cache()
    println(f"dDateRaw.count()=${dDateRaw.count()}")
    val results = flattenResults[(Int, Int)](dDateRaw.collect)

    val supplierPaths = Array(
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER0",
      "/home/fathi/workspace/gpudb/database/SSB/scale-10/SUPPLIER5")
    val supplierRaw: GpuRDD[(Int, String)] = sc.fromColumnarFiles[(Int, String)](supplierPaths,
      1000, 1)

    supplierRaw.cache()
    println(f"supplierRaw.count()=${supplierRaw.count()}")


    val results2 = flattenResults[(Int, String)](supplierRaw.collect)

    results.take(100).foreach(x =>println(x))
    results2.take(100).foreach(x =>println(x))
    println(f"results.size=${results.size}")
    println(f"results2.size=${results2.size}")
  }

  /**
   *
   */
  ignore("collect on String on large data test") {
    /*__________________________ d_datekey. d_year _____________________________*/

    val ddatePaths = Array("/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE0"
      ,"/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE1"
      , "/home/fathi/workspace/gpudb/database/SSB/scale-10/DDATE2")
    val dDateRaw = sc.fromColumnarFiles[(Int, String, String)](ddatePaths, 639, 1)


    dDateRaw.cache()
    println(f"dDateRaw.count()=${dDateRaw.count()}")
    val results = flattenResults[(Int, String, String)](dDateRaw.collect)

    results.take(100).foreach(x =>println(x))
    println(f"results.size=${results.size}")
  }

}
