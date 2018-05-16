package com.companion.objects

object Extractor {
  
  def apply(id:Int) = id*10
  
  def unapply( idTemp:Int):Option[Int] = {
    val id = idTemp%10
    if( id == 0) Some(idTemp/10) else None
  }
}