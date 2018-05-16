package com.sample

object MixinsObject {
  
  def main(args:Array[String]):Unit={
    val d = new D();
    println(d.message)
    println(d.upperMessage)
    println(d.lowerMessage)
  }
}