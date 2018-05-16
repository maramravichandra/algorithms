package com.nested.currying.caseclass

import com.fasterxml.jackson.databind.ObjectMapper
import org.json.JSONObject

object MainObject {
  
  //Nested Methods
  def factorial( x:Int ):Int={
    
    def fact( x:Int, value:Int ):Int={
      println( s""" x:$x, value :$value""")
      if( x <= 1 ) return value
      else return fact( x-1, x*value);
    }
    
    fact(x,1)
  }
  
  
  def matching( temp:Temp ):Boolean= temp match{
    case Temp(1,2) => return true;
    case _ => return false
    
  }
  
  //Currying
  
  def mod( n:Int)(x:Int) = {
    println(s""" n:$n, x:$x """ )
    ((x%n) == 0)
  }
  
  
  def filter( list:Array[Int], f:Int =>Boolean):Array[Int]={
    
    if( list.isEmpty ) list
    else if(  f( list.head) ) list ++ filter( list.tail,f) // f(2)(list.head)
    else filter( list.tail, f)
  }
  
  def main(args:Array[String]){
    
    println( factorial(3) );
    
    val arr = Array(10,1,2,5,4,6);
    val arrTmp = filter( arr, mod(2))
    arrTmp.foreach(print);
    println()
   println(  CurryingObj1.getXvalue( CurryingObj1.y(arrTmp.head)) )
   
   
   val temp = new Temp(1,2)
    
    val temp2 = new Temp(1,2)
   
    val result = matching(temp);
    println( "Result",result );
    
    println( "Compare",temp == temp2 );
    
    val json = " { \"a\":1, \"b\":3} "
    val jsonObj = new JSONObject(json);
    println(jsonObj.get("a"))
    val mapper = new ObjectMapper()
    val tempCaseClass = mapper.readValue(json, classOf[Temp])
    println( tempCaseClass )
    
    
    
    
   
  }
}