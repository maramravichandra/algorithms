package com.sample

class IteratorClass(toTmp:Int) extends Iterator{
  
  {
    to = toTmp;
  }
  
  def printCurrent = printCurrentValue(getCurrent())
  
}