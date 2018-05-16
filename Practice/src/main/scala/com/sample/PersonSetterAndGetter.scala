package com.sample

class PersonSetterAndGetter {
  
  private var _name = "";
  private var _age  = 0.0F;
  
  
  def name = _name;
  def name(name:String) = _name=name;
  
  def age = _age;
  def age(age:Float) = {
    if( age > 130 ) printErrorMessage else _age=age;
  }
  
  private def printErrorMessage = println("Given age exceeded the limit")
  
  override def toString():String={
    s"""Name = $name, Age = $age""";
  } 
}