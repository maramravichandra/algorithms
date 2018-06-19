package com.companion.objects

class CompanionObject{
  
  private var name = "CompanionObject";
  def getName = name;
  private def setName(nameTemp:String) = name=nameTemp;
}

object CompanionObject {
  
  private val cobj = new CompanionObject
  
  def main(args: Array[String]): Unit = {
    
    println("Name",cobj.getName);
    cobj.setName("SET NAME")
    println("Name",cobj.getName);
  }
}