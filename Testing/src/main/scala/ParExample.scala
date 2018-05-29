import java.util.ArrayList
import scala.collection.mutable.ListBuffer


object ParExample {
  
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[Int]();
    
    for( i <- 1 to 100 ){
      list += i;
    }
    
    val pars = list.par;
    
    pars.foreach(waiting);
    println("************ SOME THING **************");
  }
  
  def waiting( value:Int ){
    
    for( j <- 0 to 100000000 ){
      
    }
    
    println(value);
  }
}