package com.pattern.matching

sealed class SealedClass
case class SubSealed1(x:Int) extends SealedClass
case class SubSealedClass2(x:Int) extends SealedClass{
  def getMessage = "This is Sealed Class 2"
}