package com.sorting.algorithm

class SelectionSort extends SortingTrait {
  
  override def doSorting(){
    var temp = 0;
    val maxLenght = numbers.length - 1;
    
    for( i <- 0 to maxLenght ){

    	var minValueIndex = i;
    	for( j <- (i+1) to maxLenght ){
    		if( numbers(minValueIndex) > numbers(j) )
    			minValueIndex = j;
    	}

    	val temp = numbers(minValueIndex);
    	numbers(minValueIndex) = numbers(i);
    	numbers(i) = temp;
    }
  }
}