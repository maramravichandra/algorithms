package com.oalva.cobra.hdf

import scala.collection.mutable.{ HashMap, ArrayBuffer }
import scala.io.Source._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object flagProcessor {

  var flagOneMapB: Broadcast[HashMap[String, String]] = null
  var flagTwoMapB: Broadcast[HashMap[(String, String), String]] = null
  var bbwFlagOneKeywordsB: Broadcast[Array[String]] = null
  var bellFlagOneKeywordsB: Broadcast[Array[String]] = null
  var chatrFlagOneKeywordsB: Broadcast[Array[String]] = null
  var fidoFlagOneKeywordsB: Broadcast[Array[String]] = null
  var freedomFlagOneKeywordsB: Broadcast[Array[String]] = null
  var kodooFlagOneKeywordsB: Broadcast[Array[String]] = null
  var publicFlagOneKeywordsB: Broadcast[Array[String]] = null
  var rogersFlagOneKeywordsB: Broadcast[Array[String]] = null
  var telusFlagOneKeywordsB: Broadcast[Array[String]] = null
  var virginFlagOneKeywordsB: Broadcast[Array[String]] = null
  var shawFlagOneKeywordsB: Broadcast[Array[String]] = null

  val intiliazeFlagProperty = (sc: SparkContext) => {

    var flagOnePath = "/home/cloudera/Desktop/MOBY/flagOneKeywordMapping.csv";//sc.getConf.get("spark.cobra.flagOnePath")
    var flagTwoPath = "/home/cloudera/Desktop/MOBY/flagTwoKeywordMapping.csv";//sc.getConf.get("spark.cobra.flagTwoPath")
    
    val flagOneLines = fromFile(flagOnePath).getLines()
    //val flagOneLines = fromFile("/data/landing/mmarimuthu/home/mmarimuthu/freedomWorks/cobraProperties/flagOneKeywordMapping.csv").getLines()

    var flagOneMap: HashMap[String, String] = new HashMap()

    for (line <- flagOneLines) {
      val cols = line.split(",").map(_.trim())
      flagOneMap += ((cols(0), cols(1)))
    }

    var bbwFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "bbw").keys.toArray
    var bellFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "bell").keys.toArray
    var chatrFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "chatr").keys.toArray
    var fidoFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "fido").keys.toArray
    var freedomFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "freedom").keys.toArray
    var kodooFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "kodoo").keys.toArray
    var publicFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "public").keys.toArray
    var rogersFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "rogers").keys.toArray
    var telusFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "telus").keys.toArray
    var virginFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "virgin").keys.toArray
    var shawFlagOneKeywords: Array[String] = flagOneMap.filter(_._2 == "shaw").keys.toArray

    val flagTwoLines = fromFile(flagTwoPath).getLines()
    //val flagTwoLines = fromFile("/data/landing/mmarimuthu/home/mmarimuthu/freedomWorks/cobraProperties/flagTwoKeywordMapping.csv").getLines()

    var flagTwoMap: HashMap[(String, String), String] = new HashMap()

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
        flagTwoMap += (((col(i), row(j)), x))
        j += 1
      }
      i += 1
    }
    
    flagOneMapB = sc.broadcast(flagOneMap)
    flagTwoMapB = sc.broadcast(flagTwoMap)
    bbwFlagOneKeywordsB = sc.broadcast(bbwFlagOneKeywords)
    bellFlagOneKeywordsB = sc.broadcast(bellFlagOneKeywords)
    chatrFlagOneKeywordsB = sc.broadcast(chatrFlagOneKeywords)
    fidoFlagOneKeywordsB = sc.broadcast(fidoFlagOneKeywords)
    freedomFlagOneKeywordsB = sc.broadcast(freedomFlagOneKeywords)
    kodooFlagOneKeywordsB = sc.broadcast(kodooFlagOneKeywords)
    publicFlagOneKeywordsB = sc.broadcast(publicFlagOneKeywords)
    rogersFlagOneKeywordsB = sc.broadcast(rogersFlagOneKeywords)
    telusFlagOneKeywordsB = sc.broadcast(telusFlagOneKeywords)
    virginFlagOneKeywordsB = sc.broadcast(virginFlagOneKeywords)
    shawFlagOneKeywordsB = sc.broadcast(shawFlagOneKeywords)   

  }

  def getMatchingWord = (tweetMessage: String) => {
    var firstOccurenceBW = ""
    var tweetKeyword = ""
    var bbwFlag = 0
    var bellFlag = 0
    var chatrFlag = 0
    var fidoFlag = 0
    var freedomFlag = 0
    var koodoFlag = 0
    var publicFlag = 0
    var rogersFlag = 0
    var telusFlag = 0
    var virginFlag = 0
    var shawFlag = 0

    val tweetCleanedMessage = tweetMessage.replaceAll("[^\\p{L}\\p{Nd}]+", " ").toLowerCase()
    val tweetwords1 = tweetCleanedMessage.split(" ")
    val tweetwords2 = for (Array(a, b, _*) <- tweetwords1.sliding(2).toArray) yield (a + " " + b)
    val tweetwords = tweetwords1 ++ tweetwords2

    for (tweetword <- tweetwords) {
      if (flagOneMapB.value.contains(tweetword)) {
        if (firstOccurenceBW == "") {
          firstOccurenceBW = flagOneMapB.value.getOrElse(tweetword, "")
        }
        if (bbwFlagOneKeywordsB.value.contains(tweetword)) {
          bbwFlag = 1
        }
        if (bellFlagOneKeywordsB.value.contains(tweetword)) {
          bellFlag = 1
        }
        if (chatrFlagOneKeywordsB.value.contains(tweetword)) {
          chatrFlag = 1
        }
        if (fidoFlagOneKeywordsB.value.contains(tweetword)) {
          fidoFlag = 1
        }
        if (freedomFlagOneKeywordsB.value.contains(tweetword)) {
          freedomFlag = 1
        }
        if (kodooFlagOneKeywordsB.value.contains(tweetword)) {
          koodoFlag = 1
        }
        if (publicFlagOneKeywordsB.value.contains(tweetword)) {
          publicFlag = 1
        }
        if (rogersFlagOneKeywordsB.value.contains(tweetword)) {
          rogersFlag = 1
        }
        if (telusFlagOneKeywordsB.value.contains(tweetword)) {
          telusFlag = 1
        }
        if (virginFlagOneKeywordsB.value.contains(tweetword)) {
          virginFlag = 1
        }
        if (shawFlagOneKeywordsB.value.contains(tweetword)) {
          shawFlag = 1
        }
      }
    }
    val flagTwoWord = getMatchingF2Word(tweetCleanedMessage)
    val flags = firstOccurenceBW + "#" + flagTwoWord + "#" + bellFlag + "#" + rogersFlag + "#" + freedomFlag + "#" + telusFlag + "#" + chatrFlag + "#" + fidoFlag + "#" + koodoFlag + "#" + publicFlag + "#" + shawFlag + "#" + virginFlag + "#" + bbwFlag
    flags
  }

  val getMatchingF2Word = (tweetMessage: String) => {
    var word = ""
    var i = 0
    var j = 0
    ///var x = ""
    val keywords = flagTwoMapB.value
    var key = ("", "")
    for (combination <- keywords.keys) {
      if (tweetMessage.contains(combination._1) && tweetMessage.contains(combination._2))
        if (i == 0 && j == 0) {
          //x=combination._1
          key = combination
          i = tweetMessage.toLowerCase().indexOf(combination._1)
          j = tweetMessage.toLowerCase().indexOf(combination._2)
        } else if ((tweetMessage.indexOf(combination._1) < i) && (tweetMessage.indexOf(combination._2) < j)) {
          key = combination
          i = tweetMessage.toLowerCase().indexOf(combination._1)
          j = tweetMessage.toLowerCase().indexOf(combination._2)
        }
    }
    word = keywords.getOrElse(key, "")
    word
  }

  val returnMasterFlag = (flagOne: String, flagTwo: String) => {
    var word = "others"
    if (flagOne != null && flagOne != "") {
      word = flagOne
    } else if (flagTwo != null && flagTwo != "") {
      word = flagTwo
    }
    word
  }

  val getMatchingWordUDF = udf(getMatchingWord)

}