import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{monotonically_increasing_id, explode, first}

object TupleToDF {
  
  case class Test(map:Map[String,String]);
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("TupleToDF").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tuples:List[(String,String)] = List(("name","Ravichandra"),("id","12345"),("surname","maramreddy"),("designation","SrBigdataEngineer"),("Company","Kogentix"));
    val tmp = tuples.toMap;
    val test = new Test(tmp);
    val list:List[Test] = List(test);
    var df = list.toDF();
    df.show();
    
    df = df.withColumn("id", (monotonically_increasing_id()))
	  .select($"id", explode($"map"))
	  .groupBy("id")
	  .pivot("key")
	  .agg(first("value"));
    
    df.show();
    
    
    
  }
}