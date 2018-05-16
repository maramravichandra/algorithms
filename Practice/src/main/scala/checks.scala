
package com.arl_cleaning_rules.standardising

/**
 * @author Meghamala Bheri
 */

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType,IntegerType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Logger,LogManager,Level,PropertyConfigurator}
import org.apache.spark.sql.Column
//import com.amazonaws.services.s3.AmazonS3Client
//import com.amazonaws.auth.BasicAWSCredentials
//import com.amazonaws.services.s3.model.{ListObjectsV2Request}
import org.apache.spark.sql.functions._
//import com.amazonaws.services.s3.model.{AmazonS3Exception, ListObjectsV2Request, ObjectMetadata}
//import com.amazonaws.services.s3.model.GetObjectRequest
//import com.amazonaws.services.s3.model.ListObjectsRequest
//import com.amazonaws.services.s3.model.ObjectListing
//import com.amazonaws.services.s3.model._
import scala.collection.JavaConverters._
import org.apache.log4j.{Logger,LogManager,Level,PropertyConfigurator}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Timestamp
//import java.sql.Date
import java.util.Calendar
import org.joda.time.DateTime
import java.time._
import java.util.GregorianCalendar;
import org.joda.time.DateTimeZone.UTC
import scala.util.Try

object checks {  //initialize spark session
  def main(args:Array[String]){
 
  val spark = SparkSession
                .builder
                .appName("load_agent_licappt_data").master("local")
                .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
                .getOrCreate()
                
 val context = spark.sparkContext      
 val sqlContext = spark.sqlContext;
  
  
//  def initS3Client(): AmazonS3Client = {  //initialize s3 connection
//    val credential = new BasicAWSCredentials("AKIAI2UJPRO6GTN7BFNQ", "qhSURM7vXIdiy89WHgRjwqcWj7VVUKBpCy1Hgx1i")
//    return new AmazonS3Client(credential)
//    //return new AmazonS3Client()
//    }
   println("connection successful")


import spark.implicits._


   System.setProperty("hadoop.home.dir", "C:\\winutils\\")
   
   
   context.hadoopConfiguration.set("fs.s3a.access.key", "AKIAI2UJPRO6GTN7BFNQ");
   context.hadoopConfiguration.set("fs.s3a.secret.key", "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");
       
   context.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      
       
       //val df= sqlContext.read.parquet("s3a://464816863426us-east-1glbcocuoeoubudlkeat-lak-raw//dev//platform//advisor//arl//agent//source=P05//load_date=2018-04-25//batch_id=33e61f76-1dbd-4448-97bf-4f75b8af83ce")
  val df = spark.read.format("CSV")          //load the data from s3
                .option("delimiter","|")
                .option("header","true")
                //.load("s3a://tt-data-landing-arl//dev//platform//advisor//arl//agent//source=DSS//loaddate=2018-04-19//batchid=4c5d588f-0c42-40e1-8b4b-128354c96785//dssagents_2018-04-19.txt")
                //.load("s3a://tt-data-landing//platform//region='DEV'//source='P03'//load_date='2018-04-10'//batch_id='e7e07303-1846-4219-8c6c-90ff218b2512'//file='agent'//p03agents_2018-04-10.txt")
                .load("C:\\Users\\mbheri\\Desktop\\dssagents_2018-04-19.txt")
                //.load("s3a://464816863426us-east-1glbcocuoeoubudlkeat-lak-raw//dev//platform//advisor//arl//agent//source=P05//load_date=2018-04-25//batch_id=33e61f76-1dbd-4448-97bf-4f75b8af83ce")
               // val s3Client = initS3Client()
   println("data loaded")
      // df.show()
   //df.printSchema()
   
  
       
  def   Upper_RemoveAllWhitespace_lastname(lastname:String): Option[String] ={ //function to convert letters in to upper case and remove alphanu
   val str = Option(lastname).getOrElse(return None)
    Some(str.toUpperCase().replaceAll("[^A-Z]",""))
    }
  val Upper_RemoveAllWhitespace_lastnameUDF = udf[Option[String], String](Upper_RemoveAllWhitespace_lastname) // UDF for last name column function call
       
       
       
    
  def   RemoveAllWhitespace_businessname(businessname:String): Option[String] ={ //function that removes all white spaces from business name column
      val str = Option(businessname).getOrElse(return None)
      Some(str.toUpperCase())
       }
      
  val RemoveAllWhitespace_businessnameUDF = udf[Option[String], String]( RemoveAllWhitespace_businessname)
      
    


      
  def ConvertStringToDate(dateofbirth: String): String={  //Function that converts string dob to YYYY-MM-DD format
       
        val existingFormat = new SimpleDateFormat("yyyyMMdd")
        val reqFormat = new SimpleDateFormat("yyyy-MM-dd")
        val strDate = reqFormat.format(existingFormat.parse(dateofbirth))
       strDate
        }

  val ConvertStringToDateUDF = udf(ConvertStringToDate _)
  
 
     
  println("data cleaning successful")

  val dojUDF = udf( getDataOfBirth _);
    val addDobDf = df.withColumn("DOB", dojUDF($"dateofbirth"))
    addDobDf.show()
    
    val dojValidationUDF = udf( validateDob _);
    val finalDF = addDobDf.withColumn("isvalidDOB", dojValidationUDF($"DOB"))
    finalDF.show()
    

  val newDF = finalDF.withColumn("current_date",current_date().cast("String"))
                .withColumn("c_lastname", Upper_RemoveAllWhitespace_lastnameUDF(col("lastname")))
                .withColumn("c_businessname", RemoveAllWhitespace_businessnameUDF(col("businessname")))
                .withColumn("lastname_validityflag", when($"lastname".isNull or $"lastname" === "", false).otherwise(true))
                  
                
                

                   

                
//newDF.createTempView("agent")
    
   newDF.show()
    
   newDF.printSchema()
  
       
   // newDF .write.format("csv").option("delimiter","|").option("header","true").save("s3a://tt-data-landing//test//platform//advisor//arl//agent/po31.txt")
    println("data uploaded successfully to destination bucket")
  }
  
  def getDataOfBirth(dob:String)={
    if( Try(dob.toInt).isFailure || dob.toInt == 0 || dob.trim().equalsIgnoreCase("00000000")) "1880-01-01";
    else{
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val date = sdf.parse(dob)
      
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
      sdf1.format(date);
    }
  }
  
  def validateDob(dob:String)={
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dobDate = sdf.parse(dob);
    val currentDate = new Date();
    
    if( currentDate.getTime < dobDate.getTime ){
      false
    }else{
      
      val years = currentDate.getYear - dobDate.getYear;
      if( years < 130 ) true
      else false
    }
  }
}
