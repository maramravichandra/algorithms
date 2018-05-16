package com.sample

object TraitObject {
  
  def main(args:Array[String]):Unit={
    
    val iterClass = new IteratorClass(3);
    iterClass.next();
    iterClass.printCurrentValue(iterClass.getCurrent());
    
    iterClass.next();
    iterClass.printCurrentValue(iterClass.getCurrent());
    
    iterClass.next();
    iterClass.printCurrentValue(iterClass.getCurrent());
    
    iterClass.next();
    iterClass.printCurrentValue(10);
    
    val enum = new Enumerator;
    enum.next();
    enum.printCurrent;
    
    enum.next();
    enum.printCurrent;
    
    enum.next();
    enum.printCurrent;
    
    enum.next();
    enum.printCurrent;
    
    var iter:Iterator = new IteratorClass(3);
    iter = new Enumerator
    
    
//    val lion = new Lion()
//    val tiger = new Tiger()
//    
//    
//    
//    val animals = Array(lion,tiger)
//    
//    animals.foreach{ animal =>
//      
//      animal.features();
//      animal.voice()
//      
//    }
    
    
    
    
    
  }
}