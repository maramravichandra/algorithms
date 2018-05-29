package com.sorting

object BubbleSort {
  
  def main(args: Array[String]): Unit = {
    val numbers:Array[Int] = Array(5,1,12,-5,16);
    
    println("Before Sorting :: ",numbers.foreach( x => print(x+" ")) );
    val size = numbers.size;
    
    var k = 0;
    
    for( i <- 0 to size-1 ){
    	for( j <- 0 to size -2 ){
    		if( numbers(j) > numbers(j+1) ){
    			k = numbers(j);
    			numbers(j) = numbers(j+1);
    			numbers(j+1) = k;
    		}

    		println();
    		numbers.foreach( x => print(x+" "))
    	}
      
    }
    
    println("After Sorting :: ",numbers.foreach( x => print(x+" ")) );
    
  }
}