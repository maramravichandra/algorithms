package josnHTMLTagRemover;

import org.json.JSONObject
import org.json.JSONArray
import org.json.simple.parser.JSONParser
import java.io.FileReader
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level;
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArrayBuilder
import scala.collection.mutable.WrappedArray
import org.json.JSONObject

object JsonReader {
  
  val sparkSession:SparkSession = SparkSession.builder().master("local")
                                 .appName("JsonReader").getOrCreate();
  val sqlContext:SQLContext    = sparkSession.sqlContext;
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  
  case class Education(additionalDetails:String,degree:String,description:String,endDate:String,id:String,orgProfileUrl:String,schoolName:String,startDate:String);
  
  def main(args: Array[String]): Unit = {
    
    var content = sqlContext.read.parquet("C:/Users/KOGENTIX/Documents/My Received Files/part-r-00011-ebc59f41-c1f9-4589-b9b1-6ec3bbc59776.gz.parquet");
    val schema = content.schema;
    content.printSchema();
    
    
    val tempDF = content.where(col("education.degree").getItem(0) like "<%" );//"education['degree'] like '<%'"
    println("COunt",tempDF.count());
    tempDF.show(false);
    
    
    def removeHTMLTag(values:Seq[Education]):Seq[Education]={
      
      val tmp = values.seq
      
      val educations = new Array[Education](tmp.length);
      for( i <- 0 to tmp.length-1 ){
        val edu = tmp(i).asInstanceOf[Row];
        try{
             educations(i) = new Education(edu.getAs[String]("additionalDetails"),edu.getAs[String]("degree").replace("\\<.*?\\>", ""),edu.getAs[String]("description"),edu.getAs[String]("endDate"),edu.getAs[String]("id"),edu.getAs[String]("orgProfileUrl"),edu.getAs[String]("schoolName"),edu.getAs[String]("startDate"));
          }catch {
            case t: Exception =>{
             educations(i) = new Education(null,null,null,null,null,null,null,null);
            }
          }
      }
      
      return educations.toSeq;
    }
    
    val htmlRemover = udf(removeHTMLTag _);
    
    var df = content.select( col("education"));
    df.show(false);
    
    df = content.select( htmlRemover(col("education")));
    df.show(false);
    
    content = content.withColumn("education", htmlRemover(col("education")));
    content.show(false);
    
    
  }
}