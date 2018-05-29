

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap

object HDFCobraAspectScorer2 {
  
  
  var sc:SparkContext = null;
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hdfCobraAspectScorer2")
    sc = new SparkContext(conf)
    val hiveContext=new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._ 

    val aspectBaseDF = hiveContext.sql("select tweet,id from tweets");
    aspectBaseDF.registerTempTable("BASE_TABLE");
    aspectBaseDF.show;
    
    val aScorer = new aspectScorer();
    aScorer.readAskProperties(args(0));
    
    hiveContext.udf.register("getAspectValue", aScorer.getAspectValue _ );
    
    val query = s""" SELECT id, tweet, getAspectValue(tweet,'${aScorer.rowLables}') FROM BASE_TABLE""";
    println("Query",query);
    
    val df = hiveContext.sql(query);
    df.show();

    var dfTemp =  df.select($"id", explode($"_c2"))
	  .groupBy("id")
	  .pivot("key")
	  .agg(first("value"));
	 
	 dfTemp.show();
	 
    //val aspectDF = aspectBaseDF.withColumn("aspectValueString", aScorer.getAspectValueUDF(col("TweetRawMessage"),aScorer.rowLables,aScorer.askPropertyMap)).withColumn("_tmp", split($"aspectValueString", "#")).select($"source_recordID", $"aspectValueString", $"_tmp".getItem(0).as("ce"), $"_tmp".getItem(1).as("commercial"), $"_tmp".getItem(2).as("company general"), $"_tmp".getItem(3).as("competition"), $"_tmp".getItem(4).as("events"), $"_tmp".getItem(5).as("handset"), $"_tmp".getItem(6).as("network"), $"_tmp".getItem(7).as("product/services"), $"_tmp".getItem(8).as("aspectMax")).drop("_tmp").drop("aspectValueString")

    //aspectDF.insertInto("adv_analytics.ca_aspectTesting")

  }
  
  
}