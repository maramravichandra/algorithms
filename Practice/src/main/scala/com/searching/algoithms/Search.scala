package com.searching.algoithms

import scala.actors.threadpool.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedBuffer
import java.lang.Long
import scala.actors.threadpool.TimeUnit
import scala.collection.mutable.ListBuffer


class Search(souceList:ListBuffer[Object], fieldName:String,matchValue:Any ) {
  
  var list = new ArrayBuffer[Object] with SynchronizedBuffer[Object]
  
  def getMatchedObjects():ArrayBuffer[Object]={
    
    val maxThreads   = Runtime.getRuntime().availableProcessors();
    println("Available Threads",maxThreads);
	  val taskExecutor = Executors.newFixedThreadPool(maxThreads);
	  val iterables    = souceList.grouped(maxThreads*5);
	  
	  while( iterables.hasNext ){
	    val actor = new CustomActor(iterables.next(), fieldName, matchValue, list);
	    taskExecutor.execute(actor);
	  }
	  
	  taskExecutor.shutdown();
	  taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    
    return list;
  }
  
}