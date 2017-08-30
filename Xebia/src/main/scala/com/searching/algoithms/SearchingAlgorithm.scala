package com.searching.algoithms
import akka.actor._
import scala.collection.mutable.ListBuffer
import scala.actors.threadpool.Executors
import java.lang.Long
import scala.actors.threadpool.TimeUnit
import java.util.Calendar


object SearchingAlgorithm {

	def main(args: Array[String]): Unit = {

	  var source = ListBuffer.empty[Object];
	  for( i <- 0 to 10000000 ){
	    val emp = new Employee(i);
	    source += emp;
	  }
	  
	  println("List Size",source.size);
	  println("Start Time::", System.currentTimeMillis());
	  val search = new Search(source, "designation", "Software Engineer" );
	  val list = search.getMatchedObjects();
	  println("End Time::", System.currentTimeMillis());
	  println("Number Of items found",list.size);
	  //list.foreach { x =>  println("Name:::",x.asInstanceOf[Employee].name)}
	  
	}


}