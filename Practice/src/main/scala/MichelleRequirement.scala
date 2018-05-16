import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.mllib.tree.DecisionTree
import java.text.SimpleDateFormat

object MichelleRequirement {
  
  def main(args: Array[String]): Unit = {
    
    
    val format = "mmm-dd-yy";
    val dateValue = "jan/01/90";
    
    val resultVal = getTimestamp(format, dateValue);
    println("resultVal",resultVal);
    
    
    return;
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    
    val schema = new StructType().add("id", StringType).add("name", StringType).add("lastcol", StringType).add("numcol",StringType);
    var df = spark.read.format("com.databricks.spark.csv").schema(schema).load("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\compare2.csv").orderBy("id");

    import spark.implicits._;
    val columns = df.schema.fields.filter( st => st.name.equals("id") || st.name.equals("numcol")).map( st => st.name).mkString(",");
    
    spark.udf.register("getmaxvalue", getMaxValue _);
    df.createOrReplaceTempView("TEMP_TABLE");
    df = spark.sql(s"SELECT *,getmaxvalue(concat_ws( '@@@@',$columns)) as maxvaluecol FROM  TEMP_TABLE");
    df.show();
    
  }
  
  def getMaxValue( valueTmp:String )={
    val values = valueTmp.split("@@@@");
    val convetedArr = values.map( value => if( Try( value.toString().toDouble ).isSuccess) value.toString().toDouble else 0 );
    convetedArr.max
  }
  
  
  private def getTimestamp(format:String,dateString:String):Long={
	  try{
	    val sdf = new SimpleDateFormat(format);
			val date = sdf.parse(dateString.replace("/", "-"));
			val timestamp = date.getTime;
			return timestamp;
	  }catch{
	    case exp:Exception =>
	      return -99999;
	  }
	}
}