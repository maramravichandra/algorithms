package com.companion.objects

object Test {
  
  lazy val x = 10;
  val y = 20;
  
  def main(args: Array[String]): Unit = {
    
    val z = x+y;
    
    println( x,y,z)
    
    
    
    val cobj = new CompanionObject
    println("Name",cobj.getName);
    
    val extractor = Extractor(100)
    val Extractor(extractor1) = extractor;
    println("extractor1",extractor1);
    
    extractor match{
      case Extractor(extractor) => println( extractor)
      case _ => println("Nothing");
    }
    
    
    val arr = Array(1,4,5,6);
    
    
    return;
    
    println("==Filter==")
    val arr1 = arr.filter( value => value >= 5);
    arr1.foreach( println)
    println("== for yield ===")
    val arr2 = for( value <- arr if value >= 5 ) yield value
    arr2.foreach( println )
    
    println("== map ===")
    val arr3 = arr.map( x => if(x < 5 ) 5 else x )
    arr3.foreach( println)
    
    
    
    
    
  }
}