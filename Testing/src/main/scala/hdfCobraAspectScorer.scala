package com.cobra.hdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._

object hdfCobraAspectScorer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hdfCobraAspectScorer");
    val sc = new SparkContext(conf)
    val hiveContext=new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._ 

    val aspectBaseDF = hiveContext.sql("select tweetRawMessage,source_recordID from adv_analytics.CA_Master")

    val askPropertyDF = hiveContext.read.format("com.databricks.spark.csv").options(Map("header" -> "true")).option("delimiter", ",").load("/tmp/hive/mmarimuthu/freedomSample/cobraProperties/askPropertyFile.csv");

    askPropertyDF.registerTempTable("askProperty")
    hiveContext.cacheTable("askProperty")

    val sqlContextB = sc.broadcast(hiveContext)

    

    val askWords = askPropertyDF.columns.filter(_ != "row labels").map(_.toLowerCase)
    val askWordsB = sc.broadcast(askWords)

    def matchTest(x: Int): String = x match {
      case 0 => "ce"
      case 1 => "commercial"
      case 2 => "company general"
      case 3 => "competition"
      case 4 => "events"
      case 5 => "handset"
      case 6 => "network"
      case 7 => "product/services"
    }

    val computeAspect = (aspectValueString: String) => {
      var aspectTempArray = aspectValueString.split("#").map(_.toDouble)
      var aspectSum = aspectTempArray.sum
      var i = aspectTempArray.indexOf(aspectTempArray.max)
      var aspectArray1 = aspectTempArray.map(x => x / aspectSum)
      var aspectValue = aspectArray1.mkString("#") + "#" + matchTest(i)
      aspectValue
    }

    val getAspectValue = (tweetMessage: String) => {
      val keywords = askWordsB.value
      
      var tempAskWords = ""
      var aspectValue = "0#0#0#0#0#0#0#0#others"

      if (keywords.exists(tweetMessage.toLowerCase().contains)) {
        for (word <- keywords) {
          if (tweetMessage.trim().toLowerCase().contains(word.trim().toLowerCase())) {
            var tempWord = word
            if (word.contains("'")) {
              tempWord = "`" + word + "`"
            }
            tempAskWords += tempWord + "+"
          }
        }
        tempAskWords = tempAskWords.substring(0, tempAskWords.length() - 1)
        var sqlQuery = "select (0.0+" + tempAskWords + ") as aspectValue from askProperty where trim(`row labels`) != 'grand total'"
        var tempAspectValue = sqlContextB.value.sql(sqlQuery).map(x => x.toString().replace("[", "").replace("]", "")).collect().mkString("#")
        aspectValue = computeAspect(tempAspectValue)
      }
      aspectValue
    }

    val getAspectValueUDF = udf(getAspectValue)

    val aspectDF = aspectBaseDF.withColumn("aspectValueString", getAspectValueUDF(lower(col("TweetRawMessage")))).withColumn("_tmp", split($"aspectValueString", "#")).select($"source_recordID", $"aspectValueString", $"_tmp".getItem(0).as("ce"), $"_tmp".getItem(1).as("commercial"), $"_tmp".getItem(2).as("company general"), $"_tmp".getItem(3).as("competition"), $"_tmp".getItem(4).as("events"), $"_tmp".getItem(5).as("handset"), $"_tmp".getItem(6).as("network"), $"_tmp".getItem(7).as("product/services"), $"_tmp".getItem(8).as("aspectMax")).drop("_tmp").drop("aspectValueString")

    aspectDF.insertInto("adv_analytics.CA_Aspect01")

  }

}