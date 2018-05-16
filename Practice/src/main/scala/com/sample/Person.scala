package com.sample

class Person(name:String="",age:Float=0) {
 
  override def toString():String={
    s"""Name = $name, Age = $age""";
  }
}