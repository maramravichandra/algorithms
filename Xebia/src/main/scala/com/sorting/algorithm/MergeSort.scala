package com.sorting.algorithm

class MergeSort extends SortingTrait{
  
  val tempArray:Array[Int] = new Array(numbers.length);
  
  override def doSorting(){
    mergeSort(0,numbers.length-1);
  }
  
  def mergeSort(lower:Int,higher:Int){
    
    if( lower < higher ){
      val mid = (lower+higher)/2;
      mergeSort(lower,mid);
      mergeSort(mid+1,higher);
      merge(lower,mid,higher);
    }
  }
  
  def merge(lower:Int,middle:Int,higher:Int){
    
    var lower1 = lower;
    var higher1 = middle;
    var lower2 = middle+1;
    var higher2 = higher;
    
    var index = lower1;
    
    while( lower1 <= higher1 && lower2 <= higher2 ){
      
      if( numbers(lower1) <= numbers(lower2) ){
        tempArray(index) = numbers(lower1);
        lower1 = lower1 + 1;
      }else{
        tempArray(index) = numbers(lower2);
        lower2 = lower2 + 1;
      }
      index = index + 1;
    }
    
    while( lower1 <= higher1 ){
      tempArray(index) = numbers(lower1);
      index = index + 1;
      lower1 = lower1 + 1;
    }
    
     while( lower2 <= higher2 ){
      tempArray(index) = numbers(lower2);
      index = index + 1;
      lower2 = lower2 + 1;
    }

      
    index = lower; 
    while( index <= higher ){
      numbers(index) = tempArray(index);
      index = index + 1;
    }
  }
  
}