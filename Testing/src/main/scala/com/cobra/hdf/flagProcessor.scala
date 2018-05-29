package com.cobra.hdf

import scala.collection.mutable.{HashMap,ArrayBuffer}
import scala.io.Source._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe

object flagProcessor {
  
  val flagOneLines = fromFile("C:/Users/KOGENTIX/New folder/Testing/src/main/resources/flagOneKeywordMapping.csv").getLines()
    
    var flagOneMap:HashMap[String,String] = new HashMap()
    
    for(line <- flagOneLines){
      val cols = line.split(",").map(_.trim())
      flagOneMap +=((cols(0),cols(1))); //(baseword,label)
    }
  
    println("fetching flagOneMap size " + flagOneMap.size)

    var bbwFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "bbw").keys.toArray
    var bellFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "bell").keys.toArray
    var chatrFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "chatr").keys.toArray
    var fidoFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "fido").keys.toArray
    var freedomFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "freedom").keys.toArray
    var kodooFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "kodoo").keys.toArray
    var publicFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "public").keys.toArray
    var rogersFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "rogers").keys.toArray
    var telusFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "telus").keys.toArray
    var virginFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "virgin").keys.toArray
    var shawFlagOneKeywords:Array[String] = flagOneMap.filter(_._2 == "shaw").keys.toArray
    
    val flagTwoLines = fromFile("C:/Users/KOGENTIX/New folder/Testing/src/main/resources/flagTwoKeywordMapping.csv").getLines()
    
    var flagTwoMap:HashMap[(String,String),String] = new HashMap()

    var i = 0
    var row: Array[String] = Array()
    var col: ArrayBuffer[String] = ArrayBuffer()
    var current: Array[String] = Array()
    for (line <- flagTwoLines) {
      if (i == 0) {
        row = line.split(",").map(_.trim())
      }
      current = line.split(",").map(_.trim())
      col += current(0)
      var j = 0
      for (x <- current) {
        flagTwoMap += (((col(i), row(j)), x));//3g,and_label,3g : 3g,chatr,chatr : 3g,fido,fido    
        j += 1
      }
      i += 1
    }
    
    println("fetching flagTwoMap size " + flagTwoMap.size)


    def main(args: Array[String]): Unit = {
      getMatchingWord("brad shaw tweet  on jaymehr dropped calls and freedom, bell service is bad".toLowerCase());
    }

    def getMatchingWord = (tweetMessage: String) => {
      var tweetKeyword = ""
      var flagMap:Map[String,String] = Map.empty;
      flagMap += "firstOccurenceBW" -> "";
      flagMap += "bbwFlag" -> "0";
      flagMap += "bellFlag" -> "0";
      flagMap += "chatrFlag" -> "0";
      flagMap += "fidoFlag" -> "0";
      flagMap += "freedomFlag" -> "0";
      flagMap += "koodoFlag" -> "0";
      flagMap += "publicFlag" -> "0";
      flagMap += "rogersFlag" -> "0";
      flagMap += "telusFlag" -> "0";
      flagMap += "virginFlag" -> "0";
      flagMap += "shawFlag" -> "0";
      
    
      val tweetCleanedMessage = tweetMessage.replaceAll("[^\\p{L}\\p{Nd}]+", " ").toLowerCase()
      val tweetwords1 = tweetCleanedMessage.split(" ")
      val tweetwords2 = for (Array(a, b, _*) <- tweetwords1.sliding(2).toArray) yield (a + " " + b)
      val tweetwords = tweetwords1 ++ tweetwords2

      for (tweetword <- tweetwords) {
        if (flagOneMap.contains(tweetword)) {
          if (flagMap.get("firstOccurenceBW").get == "") {
             flagMap += "firstOccurenceBW" -> flagOneMap.getOrElse(tweetword, "");
          }
          if (bbwFlagOneKeywords.contains(tweetword)) {
            flagMap += "bbwFlag" -> "1";
          }
          if (bellFlagOneKeywords.contains(tweetword)) {
            flagMap += "bellFlag" -> "1";
          }
          if (chatrFlagOneKeywords.contains(tweetword)) {
            flagMap += "chatrFlag" -> "1";
          }
          if (fidoFlagOneKeywords.contains(tweetword)) {
            flagMap += "fidoFlag" -> "1";
          }
          if (freedomFlagOneKeywords.contains(tweetword)) {
            flagMap += "freedomFlag" -> "1";
          }
          if (kodooFlagOneKeywords.contains(tweetword)) {
            flagMap += "koodoFlag" -> "1";
          }
          if (publicFlagOneKeywords.contains(tweetword)) {
            flagMap += "publicFlag" -> "1";
          }
          if (rogersFlagOneKeywords.contains(tweetword)) {
            flagMap += "rogersFlag" -> "1";
          }
          if (telusFlagOneKeywords.contains(tweetword)) {
            flagMap += "telusFlag" -> "1";
          }
          if (virginFlagOneKeywords.contains(tweetword)) {
            flagMap += "virginFlag" -> "1";
          }
          if (shawFlagOneKeywords.contains(tweetword)) {
            flagMap += "shawFlag" -> "1";
          }
        }
      }
      val output = getMatchingF2Word(tweetCleanedMessage,flagMap);
      val flagTwoWord = output._1;
      flagMap = output._2;
       val flags = flagMap.get("firstOccurenceBW").get + "#" +flagTwoWord+"#"+ flagMap.get("bellFlag").get + "#" + flagMap.get("rogersFlag").get + "#" + flagMap.get("freedomFlag").get + "#" + flagMap.get("telusFlag").get + "#" + flagMap.get("chatrFlag").get + "#" + flagMap.get("fidoFlag").get + "#" + flagMap.get("koodoFlag").get + "#" + flagMap.get("publicFlag").get + "#" + flagMap.get("shawFlag").get + "#" + flagMap.get("virginFlag").get + "#" + flagMap.get("bbwFlag").get
       println(tweetCleanedMessage)
       println(flags)
       flags
      }
      
      
   val getMatchingF2Word = (tweetMessage:String,flagMapTmp:Map[String,String]) => {
     
     var flagMap = flagMapTmp;
     var word = ""
      var i = 0
      var j = 0
      ///var x = ""
      val keywords = flagTwoMap; //(3g,and_label),3g : (3g,chatr),chatr : (3g,fido),fido 
      var key = ("","") 
      for(combination <- keywords.keys){
        
        if( tweetMessage.contains(combination._1) && tweetMessage.contains(combination._2) ){
          
          if (bellFlagOneKeywords.contains(combination._1)) {
            flagMap += "bellFlag" -> "1";
          }
          if (chatrFlagOneKeywords.contains(combination._1)) {
            flagMap += "chatrFlag" -> "1";
          }
          if (fidoFlagOneKeywords.contains(combination._1)) {
            flagMap += "fidoFlag" -> "1";
          }
          if (freedomFlagOneKeywords.contains(combination._1)) {
            flagMap += "freedomFlag" -> "1";
          }
          if (kodooFlagOneKeywords.contains(combination._1)) {
            flagMap += "koodoFlag" -> "1";
          }
          if (publicFlagOneKeywords.contains(combination._1)) {
            flagMap += "publicFlag" -> "1";
          }
          if (rogersFlagOneKeywords.contains(combination._1)) {
            flagMap += "rogersFlag" -> "1";
          }
          if (telusFlagOneKeywords.contains(combination._1)) {
            flagMap += "telusFlag" -> "1";
          }
          if (virginFlagOneKeywords.contains(combination._1)) {
            flagMap += "virginFlag" -> "1";
          }
          if (shawFlagOneKeywords.contains(combination._1)) {
            flagMap += "shawFlag" -> "1";
          }
          
          if(i == 0 && j == 0) {
            //x=combination._1
            key = combination
            i=tweetMessage.toLowerCase().indexOf(combination._1)
            j=tweetMessage.toLowerCase().indexOf(combination._2)
          }else if((tweetMessage.indexOf(combination._1)<i) && (tweetMessage.indexOf(combination._2)<j)){
            key=combination
            i = tweetMessage.toLowerCase().indexOf(combination._1)
            j = tweetMessage.toLowerCase().indexOf(combination._2)            
          }
        }
          
      }
      word = keywords.getOrElse(key, "")
      (word,flagMap);
  }
   
     val returnMasterFlag = (flagOne: String, flagTwo: String) => {
      var word = ""
      if (flagOne != null && flagOne != "") {
        word = flagOne
      } else if (flagTwo != null && flagTwo != "") {
        word = flagTwo
      }
      word
    }
   
   val getMatchingWordUDF = udf(getMatchingWord)
     
}