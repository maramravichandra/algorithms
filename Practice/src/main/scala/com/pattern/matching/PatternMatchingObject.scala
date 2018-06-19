package com.pattern.matching

object PatternMatchingObject {
  
  abstract class Person();
  case class Man( name:String,age:Float) extends Person{
    def walk = "He walks";
  }
  
  case class Women(name:String,age:Float) extends Person{
    def walk = "She walks"
  }
  
  case class Test2(x:Int,y:Double=0,z:Boolean=false) extends Test;
  
  def main(args: Array[String]): Unit = {
    
    val test2 = Test2(10);
    val test21 = Test2(20,z=true);
    val test22 = Test2(20,12);
    
    println(getMatchedString(20));
    println(getMatchedString(1));
    println(getMatchedString(100));
    
    if( test2 == test21 ){
      
    }
    var result = validatePerson(Man("Ravi",100))
    println( result );
    println( validatePerson(Man("Ravi",140)) );
    println( validatePerson(Man("Seenu",140)) );
    
    println( getName )
    
    var typeName = matchObjectType(Man("Anil",30))
    println( typeName );
    
    typeName = matchObjectType(Women("Anoth",20))
    println( typeName );
   
    
    val str =  s"""$typeName
      | $typeName
      | $typeName
      | $typeName
      | $typeName
      | $typeName
      | $typeName""".stripMargin
    println(str)
  }
  
  def getName = false
  
  def getMatchedString( y:Int ) = y match {
      
      case 1 => "One"
      case 5 => "Five"
      case 10 => "Ten"
      case 20 => "Twenty"
      case _ => "No Matching Found"
    }
  
  def validatePerson(person:Person) = person match{
    
    case a:Man if( a.age > 130 ) => s"Person ${a} has invalid age, age is ${a.age}"
    case b:Man if( b.age < 130 ) => s"Person ${b.name} has valid age, age is ${b.age}"
    case _ => "No Person Found"
    
  }
  
  
  def matchObjectType( person:Person ) = person match {
    case x:Man => x.walk
    case y:Women => y.walk
    case _ => "Person type is Transgender"
  }
  
  
}