package com.searching.algoithms

class Employee(i:Int) {
  val id = Math.max(100,Math.round(i/10));
  val name:String = "Ravi"+Math.max(1,Math.round(i/10));
  val sal:Double = 10000 + Math.max(1,Math.round(i/10)) * i;
  val designation:String = if( sal > 30000 ) "Software Engineer" else "Associate Software Engineer";
}