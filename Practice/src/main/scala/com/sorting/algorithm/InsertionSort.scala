package com.sorting.algorithm

class InsertionSort extends SortingTrait{
  
  override def doSorting(){
    
    val maxLength = numbers.length - 1;
    
    for( i <- 1 to maxLength ){
      val current = numbers(i);
      var j = i;
      while( j > 0 && numbers(j - 1) > current ){
    	  numbers(j) = numbers(j - 1);
    	  j = j - 1;
      }
      
      numbers(j) = current;
    }
  }
  
}