
import scala.collection.mutable.{ HashMap, ArrayBuffer }
import scala.io.Source._
import org.apache.commons.lang3.math.NumberUtils
import java.util.ArrayList
import scala.collection.SortedMap;
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast.Broadcast

class aspectScorer extends Serializable{

  var bMap: Broadcast[HashMap[(String, String), Double]] = null;
  var askPropertyMap: HashMap[(String, String), Double] = new HashMap();
  var rowLables:String = "";
  
  def readAskProperties(propertyFile:String){
  
  	val askProperty = fromFile(propertyFile).getLines();
  	var i = 0;
  	var row: Array[String] = Array();
  	var col: ArrayBuffer[String] = ArrayBuffer();
  	var current: Array[String] = Array();
  	for (line <- askProperty) {
  		if (i == 0) {
  			row = line.split(",").map(_.trim());
  			rowLables = row(0);
  		}
  		current = line.split(",").map(_.trim())
  				col += current(0);
  		var j = 0
  				for (x <- current) {
  					askPropertyMap += (((col(i), row(j)), NumberUtils.toDouble(x)));
  					j += 1;
  				}
  		i += 1;
  	}
  	
  	bMap = HDFCobraAspectScorer2.sc.broadcast(askPropertyMap);
  }
  
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
  

  val getAspectValueUDF = udf(getAspectValue(_:String,_:String));
  
  
  def getAspectValue(tweetMessage: String,rowLables:String): SortedMap[String, String] = {
    
    val keywords = bMap.value;
    var matchingKeywords: SortedMap[String, Double] = SortedMap.empty
    
    for(combination <- keywords.keys){
      if(tweetMessage.contains(combination._2)){
        if(combination._1.trim().equalsIgnoreCase(rowLables) == false  && combination._1.equalsIgnoreCase("grand total") == false ){
          println(combination);
          println(keywords.get(combination).get.toDouble);
          
          var value:Double = keywords.getOrElse(combination, 0)
          if( matchingKeywords.contains(combination._1)){
            value = value + matchingKeywords.get(combination._1).get;
          }
          
          matchingKeywords += combination._1 -> value ;
        }
      }
    }
    
    matchingKeywords.groupBy ( _._1) .map { case (k,v) => k -> v.map(_._2).sum };
    println(matchingKeywords.size);
    val sum = matchingKeywords.values.sum;
    matchingKeywords = matchingKeywords.map( pair => (pair._1,pair._2/sum));
    
    var maxkey = ""
    if (matchingKeywords.size != 0) {
      maxkey = matchingKeywords.maxBy(_._2)._1;
    }
    
    var finalMap = matchingKeywords.map( pair => ( pair._1,pair._2.toString()));
    finalMap += "aspectmax" -> maxkey;
    finalMap
  }

  case class Test(map:SortedMap[String, Double]) extends Serializable

}