import org.apache.commons.lang3.math.NumberUtils


object DS {
  
  def main( args:Array[String] ){
    
    val s = "10,10";
    println( "sum",sumValues(s));    
    
    val app1 = APP(1,"Ravi");
    val app2 = APP(2,"Ravi123");
    
    val arr = Array(app1,app2);
    
    val values = app1.getClass.getDeclaredField("name").getGenericType
    println( values.isInstanceOf[String] );
    
  }
  
  def sumValues(s:String):String={
    val sums = s split "," map(_.toLong) sum;
    return sums.toString()
    
  }
  
  case class APP(id:Int,name:String)
}