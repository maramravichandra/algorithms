

package com.sample

object Sample {
  
  var intVariable = 0;
  val xyz = 0;
  val longVariable = 9703066626L;
  val doubleVariable = 0.0;
  val floatVariable = 0.0F;
  val stringVariable = "";
  var arr:Array[Int] = new Array(5);
  val booleanVariable = false;
  
  var anyVariable:Any = null;;
  
  def main(args:Array[String]){
    
    intVariable = 19;
    
    anyVariable = "Anil";
    println( anyVariable );
    
    anyVariable = 12;
    println( anyVariable );
    anyVariable = true;
    println( anyVariable );
    
    anyVariable = new Person(name="Rajesh")
    println( anyVariable );
    return;
    val longTemp:Long = longVariable;
    
    println( arr.length );
    arr(0) = 0;
    arr(1) = 0;
    arr(2) = 2;
    arr(3) = 3;
    arr(4) = 5;
    
    println(arr.foreach(println))
    
    println(intVariable.toString)
    val person = new Person("Ravi",29);
    println("Person",person);
    
    val namedPerson = new Person(name="Ravi");
    println("Named Person",namedPerson);
    
    val agedPerson = new Person (age=150);
    println("agedPerson",agedPerson);
    
    
    val person1 = new PersonSetterAndGetter;
    person1.name("Ravi")
    person1.age(29)
    println("PersonSetterAndGetter",person1);
    
    val person2 = new PersonSetterAndGetter;
    person2.name("Seenu")
    person2.age(150)
    println("person2",person2);
    
    println( "X",x)
  }
  
  def x = 0;
  
  
}