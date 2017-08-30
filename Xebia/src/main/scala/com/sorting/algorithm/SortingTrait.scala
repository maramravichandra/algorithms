package com.sorting.algorithm

trait SortingTrait {
  
  val numbers:Array[Int] = new SourceInput().getSourceArray();
  val startTime = System.currentTimeMillis();
  def doSorting(){}
  def calculateTime(){
    println("Time taken to execute :: ", System.currentTimeMillis() - startTime );
    println(numbers.mkString(","));
  }
}