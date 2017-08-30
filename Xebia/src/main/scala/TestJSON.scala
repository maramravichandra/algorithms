import org.json.JSONObject

import scala.reflect.runtime._


object TestJSON {
  
  def main(args: Array[String]): Unit = {
    
    val test = new Test();
    test.calculate(10,20);
    
    
    val list = for( i <- 0 to 100 )yield{ if(i%2 == 0) i else "NA"};
    println("list,",list.filter { x => x != "NA" });
    
    val testing = UnApply(20);
    println("testing",testing);
    
    println("testing",UnApply.unApply(20));
    
//    val arr = Array(1,2,3,4,5,6);
//    val index = arr.indexOf(5);
//    println(index);
//    
//    val tempObj:Object = new Test();
//   val fld = tempObj.getClass.getDeclaredField("name");
//   fld.setAccessible(true);
//   println( fld.get(tempObj).toString().toUpperCase() == "ravi".toUpperCase() )
  }
  
  def getUpdatedJSON(json:JSONObject){
    json.put("lastname", "maramreddy");
  }
}