import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.log4j.Logger
import org.apache.log4j.Level;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType
import scala.collection.mutable.Map
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import org.json.JSONObject
import com.google.gson.Gson


object ExceptTesting {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  
  
 

  
  def main( args:Array[String] ){
    
    val jsonStr = Source.fromFile("C:\\Users\\KOGENTIX\\Desktop\\json\\input.json").getLines().mkString("")
    println(jsonStr)
    
    val gson = new Gson()
    val jsonObj = gson.toJson(jsonStr)
    
    
    val json = new JSONObject(jsonStr)
    val phar = json.getJSONObject("pharmacyClaim")
    println(phar)
    val pharmacyClaimIdentifier = phar.getJSONObject("pharmacyClaimIdentifier")
    println(pharmacyClaimIdentifier)
    return
    val str = "SELECT (processing_product_type_cd~src_sys_feed_id~ACCT_EVENT_TRANSACTION_ID~SOURCE_SYSTEM_CD~service_feed_id~ext_src_sys_feed_id) PRIM_KEY, query from table";
    val splitStr = str.split("PRIM_KEY,");
    val prim_key = splitStr(0).replace("SELECT", "").replace("(", "").replace(")", "")
    val query = "SELECT " + splitStr(1) 
    println("primary key", prim_key );
    println(" Query",query);
    
    return 
    val a = 5;
    var a1 =3;
    
    a1 = 23;
    
    val mutable = Map.empty[String,Int];
    for( i <- 1 until 10 ){
      mutable.put(i.toString(), i)
    }
    
    mutable.foreach(println)
    
    return;
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    
    val schema = new StructType().add("id", StringType).add("name", StringType).add("lastcol", StringType);
    
    var distDF = spark.read.format("com.databricks.spark.csv").schema(schema).load("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\compare1.csv").orderBy("id");
    var PkeyDF = spark.read.format("com.databricks.spark.csv").schema(schema).load("C:\\Users\\KOGENTIX\\New folder\\Xebia\\src\\main\\resources\\compare2.csv").orderBy("id");
    distDF.show();
    
    val primaryKeys = prim_key.replaceAll("~~", "~").split("~")
    
    def getPrimaryValue( primaykeyValue:String,index:Int )={
      val values = primaykeyValue.replaceAll("~~", "~").split("~")
      values(index)
    }
    
    val valuesUDF = udf( getPrimaryValue _)
    
    for( i <- 0 until primaryKeys.length ){
      PkeyDF = PkeyDF.withColumn(primaryKeys(i), valuesUDF(col("PRIM_KEY"),lit(i)))
    }
    
    
    return;
//     df1.show()
//    df2.show()
//    
//    df1 = df1.withColumn("unique_id", rank().over(Window.orderBy(col("id"))));
//    df2 = df2.withColumn("unique_id", rank().over(Window.orderBy(col("id"))));
    
    PkeyDF.createOrReplaceTempView("table1")
//    df2.createOrReplaceTempView("table2")
//    
//    df1.show()
//    df2.show()
    
    def setStatus( column1:Any, column2:Any )={
      var status = "";
      if( column1.toString().equalsIgnoreCase(column2.toString())){
        status = "EQUAL"
      }else{
        status = column1.toString()+" - "+column2.toString()+" : NOT- EQUAL"
      }
      
      status
    }
    
    //getStatus( a.name,b.name) as name_status, getStatus( a.lastcol,b.lastcol) as lastcol_status,
    
    spark.udf.register("getStatus", setStatus _);
    
//    val columnstemp = df1.schema.fields.filter( fld => !fld.name.equals("id"));
//    val anotherCol = columnstemp(0).name;
//    
//    val columns = columnstemp.map( fld => s"""getStatus(a.${fld.name},b.${fld.name}) as ${fld.name}""").mkString(",");
//    println(columns);
    
    
//    val dropCols = columnstemp.map(fld => fld.name+"_status")
//    
//    val whereCond = columnstemp.map( fld => fld.name + " LIKE '%: NOT- EQUAL'").mkString(" OR ");
//    
//    println(whereCond)
//    
//    val finalDF = spark.sql(s"""SELECT * FROM ( SELECT a.id,$columns FROM table1 a , table2 b where a.id=b.id ) x where $whereCond """);
//    finalDF.show(false);
//    
//    val df1Diff = spark.sql(s" select a.*, 'IN a BUT NOT IN b' as tempname from table1 a LEFT OUTER JOIN table2 b on a.id=b.id where b.$anotherCol is null");
//    val df2Diff = spark.sql(s" select b.*, 'NOT IN a BUT IN b' as tempname from table1 a RIGHT OUTER JOIN table2 b on a.id=b.id where a.$anotherCol is null");
//    
//    val finalDiff = df1Diff.union(df2Diff);
//    finalDiff.show(false);
//    finalDiff.persist(StorageLevel.MEMORY_AND_DISK)
//    
//    import spark.implicits._;
//    
//    val rowCount = finalDF.count();
//    val finalDFCols = finalDF.schema.fields.map( fld => fld.name);
//    
//    val finalDiffCols = finalDiff.schema.fields.map( fld => fld.name )
//    .filter( column => if(finalDFCols.indexOf(column) != -1 ) finalDF.select(column).filter(s"$column='EQUAL'").count == rowCount else false );
//    
//    println(finalDiffCols.mkString(","));
//    
//    val df1DistDF = finalDiff.drop( finalDiffCols: _*);
//    
//    df1DistDF.show(false);
//    
    
//    val originalCols = df1.schema.fields.map( fld => fld.name );
//    val df1Dist = df1.select( originalCols.map( column => approxCountDistinct(col(column)).alias(column)): _*);
//    val distRow = df1Dist.first();
//    
//    val finalColumns = originalCols.filter( column => distRow.getAs[Long](column) != 1);
//    val finalDf = finalDiff.select(finalColumns.map( column => columns): _*,"tempname");
//    finalDf.show(false)
    
    
    
      
    
    
//    return;
//    val finalDF3 = spark.sql(s"SELECT a.*,b.* from table1 a FULL OUTER JOIN table2 b on a.id=b.id where b.$anotherCol is null OR a.$anotherCol is null");
//    finalDF3.show()
    
    
    
    
    
    
    
  }
}