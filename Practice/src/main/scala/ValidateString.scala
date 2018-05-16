

object ValidateString {
  
  def main( args:Array[String]){
    
    val regExp = "^[a-z_A-Z0-9]*$";
    
    println(" value: Ravi_chandra", "Ravi_chandra".matches(regExp))
    println(" value: Ravi_ chandra", "Ravi_ chandra".matches(regExp))
    
     println(" value: Ravichandra", "Ravichandra".matches(regExp))
      println(" value: Ravichandra_", "Ravichandra_".matches(regExp))
     println(" value: Ravichandra_ ", "Ravichandra_ ".matches(regExp))
    
  }
}