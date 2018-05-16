package com.indix.test

import scala.io.Source
import java.net.URI
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.json.JSONObject
import java.net.URLEncoder

object Indix {
  
  def main(args: Array[String]): Unit = {
    
    //val uri = this.getClass.getResource("http://www.amazon.in/dp/B01NAKTR2H");
    
    val httpClientTimeline = new DefaultHttpClient()
    val httpResponseTimeline = httpClientTimeline.execute(new HttpGet(s"http://localhost:8080/IndixRestTest/services/timeline?url=${URLEncoder.encode("http://www.amazon.in/dp/B01NAKTR2H","UTF-8")}"))
    val entityTimeline = httpResponseTimeline.getEntity()
    
    var content = ""
    if (entityTimeline != null) {
      val inputStreamTimeline = entityTimeline.getContent();
      content = Source.fromInputStream(inputStreamTimeline).getLines.mkString;
      inputStreamTimeline.close;
    }
    
    httpClientTimeline.getConnectionManager().shutdown();
    var json = new JSONObject(content);
    println("Timeline :: " , json);
    
    val httpClientAggregate = new DefaultHttpClient()
    val httpResponseAggregate = httpClientAggregate.execute(new HttpGet(s"http://localhost:8080/IndixRestTest/services/aggregate?start=10000&end=1020000&amazon.in"))
    val entityAggregate = httpResponseAggregate.getEntity()
    
    if (entityAggregate != null) {
      val inputStreamAggregate = entityAggregate.getContent();
      content = Source.fromInputStream(inputStreamAggregate).getLines.mkString;
      inputStreamAggregate.close;
    }
    
    httpClientAggregate.getConnectionManager().shutdown();
    json = new JSONObject(content);
    println("Aggregate :: " , json);
    
  }
}