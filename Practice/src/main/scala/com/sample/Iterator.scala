package com.sample

trait Iterator {
  
  private var currentValue = 0;
  protected var to = 0;
  def hasNext() = currentValue < to
  def next() = {
    
    if( hasNext) currentValue = currentValue + 1;
    currentValue;
  }
  
  def prev() = {
    currentValue = currentValue - 1;
    currentValue
  }
  
  def getCurrent() = currentValue;
  
  def printCurrentValue(current:Int) = println("Current Value is",current);
}