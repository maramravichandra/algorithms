package com.amazon.aws

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.GetObjectRequest
import java.io.File
import scala.io.Source
import org.json.JSONObject
import org.apache.spark.sql.SparkSession
import com.amazonaws.services.s3.model.ListObjectsRequest

object AWSPractice {
  
  def main(args: Array[String]): Unit = {
    
    val accessKey = "" 
    val secretKey = "" 
    val bucketName = "" 
      // this is where it took the name of the region: 
    val urlPrefix = "https://s3-us-west-1.amazonaws.com"
      
    def isFileExistedInS3(client:AmazonS3Client,path:String)={
      try{
        client.getObjectMetadata( bucketName, path) 
        true
      }catch {
        case e:Exception=> 
        false 
      } 
    }
    
    // { "name":"Megha" }  ; key is name in below method
    def readFileFromS3(path:String):String={
      val credentials = new BasicAWSCredentials (accessKey, secretKey) 
      val client = new AmazonS3Client(credentials)
      if(!isFileExistedInS3(client, path)) return null;
      val getReq = new GetObjectRequest(bucketName, path)
      val temp = client.getObject(getReq, new File("/temp/path.json"))
      val jsonString = Source.fromFile("/temp/path.json").getLines().mkString(" ")
      val json = new JSONObject(jsonString)
      return if( json.has("source_system") ) json.getString("source_system") else null
    }
    
    val spark = SparkSession.builder().appName("AWSPractice").master("local[*]").getOrCreate()
    spark.udf.register("readFileFromS3", readFileFromS3 _ )
    
    val df = spark.sql(s"""SELECT readFileFromS3(pathcolumn) as tempcol from table """);
    
    
    import scala.collection.JavaConversions._

    val loadData = "";
    val batchId  = "";
    val credentials = new BasicAWSCredentials (accessKey, secretKey) 
    val client = new AmazonS3Client(credentials)
    
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
    val objects  = client.listObjects(listObjectsRequest).getObjectSummaries;
    val allSuccessFolders = objects.filter( obj => obj.getKey.contains(s"load_date=$loadData") && obj.getKey.split("/").last.equalsIgnoreCase("_SUCCESS")  )
                       .map( obj => obj.getKey.replace("_SUCCESS","").toString()).toArray
    val allFiles = objects.filter( obj => !obj.getKey.split("/").last.equalsIgnoreCase("_SUCCESS") ).foreach{ obj =>
      
      val file = obj.getKey.substring( 0 , obj.getKey.lastIndexOf("/"))
      val isExisted = allSuccessFolders.indexOf(file) != -1
      if( isExisted ){
        println( obj.getKey.substring( obj.getKey.lastIndexOf("/")+1 , obj.getKey.length()) ) 
      }
    }
    
    
    
    
    
  }
}