import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.Logger
import org.apache.log4j.Level;
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.SaveMode
import scala.util.Try
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source
import org.json.JSONObject
import com.google.gson.Gson

object SparkTest {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  
  def getDataOfBirth(dob:String)={
    if( Try(dob.toInt).isSuccess && dob.toInt == 0 ) "1880-01-01";
    else{
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val date = sdf.parse(dob)
      
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
      sdf1.format(date);
    }
  }
  
  def validateDob(dob:String)={
    
    
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dobDate = sdf.parse(dob);
    val currentDate = new Date();
    
    if( currentDate.getTime < dobDate.getTime ){
      false
    }else{
      
      val years = currentDate.getYear - dobDate.getYear;
      if( years < 130 ) true
      else false
    }
  }
  
  def main(args:Array[String]):Unit={
    
    val dates = s""" yyyy/mm/dd, yyyy-mm-dd,yy/mm/dd,yy-mm-dd,mm/dd/yyyy,mm-dd-yyyy,mm/dd/yy,mm-dd-yy,dd/mm/yyyy,dd-mm-yyyy,dd/mm/yy,dd-mm-yy,yyyy/dd/mm,yyyy-dd-mm,yy/dd/mm,yy-dd-mm,yyyy/mon/dd,yyyy-mon-dd,yy/mon/dd,yy-mon-dd,mon/dd/yyyy,mon-dd-yyyy,mon/dd/yy,mon-dd-yy,dd/mon/yyyy,dd-mon-yyyy,dd/mon/yy,dd-mon-yy,yyyy/dd/mon,yyyy-dd-mon,yy/dd/mon,yy-dd-mon,yyyy/month/dd,yyyy-month-dd,yy/month/dd,yy-month-dd,month/dd/yyyy,month-dd-yyyy,month/dd/yy,month-dd-yy,dd/month/yyyy,dd-month-yyyy,dd/month/yy,dd-month-yy,yyyy/dd/month,yyyy-dd-month,yy/dd/month,yy-dd-month,integer """;
    val datesArr = dates.split(",")
    println(datesArr.length);
    
    var map = Map.empty[String,String]
    val tuple = datesArr.map{ format => 
      print( s""" \"${format.trim()}\" : \"${format.trim().replace("mm","MM")}\",""" )
      (format, format.replace("mm","MM"))
    }.toMap
    
    val gson = new Gson
    val json = gson.toJson(map);
    println(json);
    
    return;
    val arr = Array("a","b","c");
    val partCol = arr.mkString(",");
    val partColName = "partColumn"
    val sql2 = s"""SELECT *, CONCAT_WS('_',$partCol) as $partColName FROM Table1 """.stripMargin;
    println(sql2);
    
    
    return;
    
    val sql = s"""SELECT c1,c2,c3 FROM tableName ORDER BY c1,c2 """.stripMargin;
    
    val primaryKey = sql.toLowerCase().split("order by")(1)
    println("primary Key", primaryKey )
    
    return;
    
    val dob = "00000000";
    println(dob.toInt);
    
    return;
    
    val spark = SparkSession.builder().master("local[*]").appName("SparkTest").getOrCreate();
    
    //val schema = new StructType().add("cust_id", StringType).add("trans_id",StringType).add("year",StringType).add("amount",StringType)
    
    val custDf = spark.read.format("com.databricks.spark.csv").option("delimeter","")
    .option("inferSchema","true").option("header","true")
    .load("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\data.txt");
    custDf.show();
    
    custDf.printSchema()
    
    //FROM HERE
    import spark.implicits._;
    val dojUDF = udf( getDataOfBirth _);
    val addDobDf = custDf.withColumn("dateofbirth", dojUDF($"dateofbirth"))
    addDobDf.show()
    
    val dojValidationUDF = udf( validateDob _);
    val finalDF = addDobDf.withColumn("isvalidDOB", dojValidationUDF($"dateofbirth"))
    finalDF.show()
    
//    return;
//    
//    val reCustDF = custDf.repartition(3)
//    println( "reCustDF numPartition", reCustDF.rdd.partitions.length );
//    
//    val coleDF = reCustDF.coalesce(5)
//    println( "coleDF numPartition", coleDF.rdd.partitions.length );
//    
//    val reColeDF = reCustDF.repartition(5)
//    println( "reColeDF numPartition", reColeDF.rdd.partitions.length );
//    import spark.implicits._;
//    
//    val test = reColeDF.map( row => row.toString())
//    test.write.mode(SaveMode.Overwrite).text("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\output\\test")
//   // reColeDF.write.mode("overwrite").format("com.databricks.spark.csv").save("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\output\\test")
//    
//    reCustDF.createOrReplaceTempView("temp_table");
//    
//    val tableDF = spark.sql("Select cust_id,count(cust_id) from temp_table group by cust_id")
//    tableDF.show()
//    
//    val sparkDF = custDf.groupBy("cust_id").count().alias("cust_id_count")
//    sparkDF.show()
    
  }
}