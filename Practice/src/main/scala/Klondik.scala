import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.Logger
import org.apache.log4j.Level;

object Klondik {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
	def main( args:Array[String]){

		val arr = Array(1,2,3,4,5,6,7,8,9);
		
		val result = for( i <- 0 until arr.length if( i > 5 ) ) yield i;
		result.foreach(print)
	}
}