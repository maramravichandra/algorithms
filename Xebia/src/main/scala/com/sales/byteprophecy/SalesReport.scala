package com.sales.byteprophecy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.collection.Iterable;
import org.apache.spark.sql.functions._;
import org.apache.log4j.Logger
import org.apache.log4j.Level;

object SalesReport {
  
  val sparkSession:SparkSession = SparkSession.builder().master("local")
                                 .appName("SalesReport").getOrCreate();
  val hiveContext:SQLContext    = sparkSession.sqlContext;
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  case class ItemValues(min:Integer, max: Integer)
  
  def main(args: Array[String]): Unit = {
    
    val data = hiveContext.read.format("com.databricks.spark.csv")
            .option("header", true)
            .option("inferSchema", true).load("C:/Users/KOGENTIX/New folder/Xebia/src/main/resources/data.txt");
    
    data.createOrReplaceTempView("DATA_TEMP");
    data.show();
    
    val rowDf = hiveContext.sql(s"""SELECT *, row_number() over (order by cust_id ) as rownum from DATA_TEMP """);    
    rowDf.createOrReplaceTempView("DATA_TEMP");
    
    val dfTMp = hiveContext.sql(s""" SELECT d1.cust_id,d1.trans_id,d1.year,d1.amount,sum(d2.amount) as sumamount FROM
                                | DATA_TEMP d1
                                | LEFT OUTER JOIN DATA_TEMP d2
                                | ON d1.rownum >= d2.rownum
                                | AND d1.cust_id = d2.cust_id 
                                | GROUP BY d1.cust_id,d1.trans_id,d1.year,d1.amount order by d1.cust_id,d1.year""".stripMargin);
    dfTMp.show();
    return;
    
    val df = hiveContext.read.format("com.databricks.spark.csv")
            .option("header", true)
            .option("inferSchema", true).load("C:/Users/KOGENTIX/New folder/Xebia/src/main/resources/sales.csv");
    
    val rdd = df.rdd;
    val result = rdd.aggregate(ItemValues(Int.MaxValue, Int.MinValue))((itemValues, row) => 
      seqOperation(itemValues, row.getAs[Int]("ITEM_VAL")), (item1, item2) => combineOperation(item1, item2));
    println("======================= USING AGGREGATE FUNCTION==========================")
    
    println("1. Minimum Item Value :::: ",result.min);
    println("1. Maximum Item Value :::: ",result.max);
    
    println("======================= USING FOLD BY KEY ================================")
    
    val rdd1 = rdd.map { item => ( item.getAs[Int]("CUSTOMER_ID"), ( item.getAs[Int]("ITEM_VAL"),item.getAs[Int]("ITEM_VAL"))) }
    val tempResult = rdd1.foldByKey((Int.MaxValue,0))((acc,element)=> {
      val min = Math.min(acc._1, element._1);
      val max = Math.max(acc._2, element._2);
      (min,max)
    })
    
    println("| CUSTOMER_ID | minValue | maxValue |")
    tempResult.sortByKey().foreach{ x =>
      println("| " + x._1 +"  | "+x._2._1+"  | "+x._2._2 +"     |");
    }
    
    println("=================== This is for Comparing ================================");
    val df1 = df.groupBy(df("CUSTOMER_ID")).agg(min(col("ITEM_VAL")).alias("minValue"),max(col("ITEM_VAL")).alias("maxValue")).orderBy(df("CUSTOMER_ID"));
    df1.show(1000);
    
  }
  
  def combineOperation(item1: ItemValues, item2: ItemValues): ItemValues = {
    ItemValues(min = Math.min(item1.min, item2.min), max = Math.max(item1.max, item2.max))
  }
  
  def seqOperation(minMax: ItemValues, value: Integer): ItemValues = {
    combineOperation(minMax, ItemValues(value, value))
  }
  
  def foldTempByKey(row:Row) : Row ={
    return row;
  }
}