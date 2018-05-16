package com.streaming.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level;

object TimeCalculationsStreaming {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  def main(args:Array[String]):Unit={
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    import spark.implicits._;
    
    val schema = new StructType().add("time", TimestampType).add("action", StringType);
    
    val df = spark.read.schema(schema).json("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\streamindata.csv")
    df.show(false);
    
    val staticCountsDF = df.groupBy($"action", window($"time","5 minutes")).count();
    staticCountsDF.createOrReplaceTempView("static_counts");
    staticCountsDF.show(false);
    
    val resultDf = spark.sql("Select action, SUM(count) as total_count from static_counts group by action");
    resultDf.show(false);

  }
}