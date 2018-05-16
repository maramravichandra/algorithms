package com.sorting.algorithm

class BubbleSort extends SortingTrait {
  
  override def doSorting(){
    var temp = 0;
    for( i <- 0 to numbers.length - 1 ){
      
      for( j <- (i+1) to numbers.length - 1 ){
        
        if( numbers(i) > numbers(j) ){
          temp = numbers(j);
          numbers(j) = numbers(i);
          numbers(i) = temp;
        }
      }
    }
  }
}