package com.ds

object Test {
  
  case class Employee(){
    var name:String = "Ravi";
    var occupation = "Software Engineer";
    var sal = 10000;
    var location = "Hyderabad";
    override def toString():String={
      return s"""Employee:\n name = $name \n occupation = $occupation \n salary = $sal \n location = $location""";
    }
  }
  
  def main(args: Array[String]): Unit = {
    val customStack = new CustomStack[Int]();
    customStack.push(20);
    println("Pushed Element is", customStack.pop())
    
    val cs = new CustomStack[Employee]();
    cs.push(new Employee);
    println("Pushed Element is", cs.pop())
    
  }
}