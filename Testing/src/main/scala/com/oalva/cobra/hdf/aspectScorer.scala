package com.oalva.cobra.hdf

import scala.collection.mutable.{ HashMap, ArrayBuffer }
import scala.io.Source._
import org.apache.commons.lang3.math.NumberUtils
import java.util.ArrayList
import org.apache.spark.sql.functions._
import scala.collection.SortedMap
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object aspectScorer {

  var askPropertyMap: HashMap[(String, String), Double] = new HashMap();
  var rowLables: String = ""
  var askPropertyMapB: Broadcast[HashMap[(String, String), Double]] = null

  val intiliazeAskProperty = (sc: SparkContext) => {
    var askPropertyPath = "/home/cloudera/Desktop/MOBY/askPropertyFile.csv";//sc.getConf.get("spark.cobra.askPropertyPath")

    //val askProperty = fromFile("/data/landing/mmarimuthu/home/mmarimuthu/freedomWorks/cobraProperties/askPropertyFile.csv").getLines()
    val askProperty = fromFile(askPropertyPath).getLines()

    var i = 0
    var row: Array[String] = Array()
    var col: ArrayBuffer[String] = ArrayBuffer()
    var current: Array[String] = Array()
    for (line <- askProperty) {
      if (i == 0) {
        row = line.split(",").map(_.trim());
        rowLables = row(0);
      }
      current = line.split(",").map(_.trim())
      col += current(0)
      var j = 0
      for (x <- current) {
        askPropertyMap += (((col(i), row(j)), NumberUtils.toDouble(x)))
        j += 1
      }
      i += 1
    }
    //askPropertyMap.foreach(println)
    askPropertyMapB = sc.broadcast(askPropertyMap)

  }

  val getAspectValue = (tweetMessage: String) => {
    val keywords = askPropertyMapB.value
    //println(keywords.size)
    var matchingKeywords: SortedMap[String, Double] = SortedMap.empty

    for (combination <- keywords.keys) {
      if (tweetMessage.contains(combination._2)) {
        if (combination._1.trim().equalsIgnoreCase(rowLables) == false && combination._1.equalsIgnoreCase("grand total") == false) {

          var value: Double = keywords.getOrElse(combination, 0)
          if (matchingKeywords.contains(combination._1)) {
            value = value + matchingKeywords.get(combination._1).get;
          }

          matchingKeywords += combination._1 -> value;
        }
      }
    }

    matchingKeywords.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).sum };
    val sum = matchingKeywords.values.sum;
    val map = matchingKeywords.map(pair => (pair._1, pair._2 / sum));

    var maxkey = ""
    if (map.size != 0) {
      maxkey = map.maxBy(_._2)._1
    }

    val aspectValueString = map.values.mkString("#") + "#" + maxkey;
    aspectValueString
  }

  val getAspectValueUDF = udf(getAspectValue)

}