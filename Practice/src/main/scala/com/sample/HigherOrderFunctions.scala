package com.sample

object HigherOrderFunctions {
  
  def main(args:Array[String]){
    
    sumCubesHigher(10, 20) // 10*10*10 = 1000 + (10+20) = 1030
  }
  
  def cube( x:Int) = x*x*x;
  def sumInts( a:Int,b:Int) = a+b;
  def sumCubes( a:Int,b:Int ) =  sumInts( cube(a),cube(b))
  
  def sumCubesHigher( a:Int,b:Int ) =  {
    val result = sum(cube, a, b)
    println( result );
    result;
  }
  
  def sum( f:Int=>Int, a:Int,b:Int ):Int={
    f(b) + sumInts(a,b)
  }
}