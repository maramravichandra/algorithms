package com.hbase.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders

object HbaseTest extends Serializable{
  
  def main(args: Array[String]): Unit = {
    
    val quorum = args(0);
    val quoramPort = 2181;
    
    val spark = SparkSession.builder().appName("ValidationTest").enableHiveSupport().getOrCreate();
    val df = spark.sql("SELECT * FROM amp.seed limit 10");
    
    
    val dfTmp = df.mapPartitions{ itr => 
    val conf = HbaseClient.createHbaseConf(quorum, quoramPort);
    HbaseClient.createTable(conf, "test", List("cf"));
    val hTable = HbaseClient.getHbaseTable(conf, "test")
    
    var map = Map.empty[(String,String),String]
    while( itr.hasNext ){
      val row = itr.next()
      map += ("cf","area") -> row.getAs[Double]("area").toString()
    }
    
    HbaseClient.upsertBatch(hTable, "test", map)
    itr }(Encoders.kryo(classOf[Row]))
    
    println( "Count", dfTmp.count() );
  }
  
  
}