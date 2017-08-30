package com.sorting.algorithm

object Sorting {
  
  def main(args: Array[String]): Unit = {
    
    var sorting:SortingTrait = null;
    
    println("========== BUBBLE SORTING ===============");
    sorting = new BubbleSort();
    sorting.doSorting();
    sorting.calculateTime();
    println("==========  END  ========================");
    
    println("========== SELECTION SORTING ============");
    sorting = new SelectionSort();
    sorting.doSorting();
    sorting.calculateTime();
    println("==========  END  ========================");
    
    
    println("========== INSERTION SORTING ============");
    sorting = new InsertionSort();
    sorting.doSorting();
    sorting.calculateTime();
    println("==========  END  ========================");
    
    println("========== MERGE SORTING ================");
    sorting = new MergeSort();
    sorting.doSorting();
    sorting.calculateTime();
    println("==========  END  ========================");
    
  }
}