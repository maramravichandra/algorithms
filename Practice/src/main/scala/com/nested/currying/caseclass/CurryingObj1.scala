package com.nested.currying.caseclass

object CurryingObj1 {
  
  val x = 10;
  def getXvalue( y:Int => Int ):Int={
    y(x)
  }
  
  def y(x:Int)(y:Int) = x*y
}