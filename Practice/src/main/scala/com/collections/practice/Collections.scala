package com.collections.practice

import scala.collection.mutable.ListMap
import scala.collection.immutable.Map
import java.util.ArrayList
import java.util.HashSet

//To support scala functionalities to java collections
import scala.collection.JavaConverters._


object Collections {
  
  val list = ListMap.empty[Int,Int]
  
  val arrList:ArrayList[Int] = new ArrayList();
  val hashset:HashSet[Int] = new HashSet()
  
  val immList = scala.collection.immutable.ListMap("yyyy-mm-dd" -> "yyyy-MM-dd")
  def main(args: Array[String]): Unit = {
    
    list += 1 -> 1
    list += 2 -> 2
    
    list.foreach(println)
    
    "yyyy-mm-dd" -> "yyyy-MM-dd"
    
    arrList.add(1)
    arrList.add(2)
    
    hashset.add(1)
    hashset.add(1)
    hashset.add(2)
    
    //need to convert to scala to iterate java collections
    hashset.asScala.foreach(println)
  }
}