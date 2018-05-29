import scala.io.Source
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.functions.{monotonically_increasing_id, explode, first}
import scala.collection.mutable.Map;
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.Partitioner




object DataPreparation {
  
  case class Test(map:Map[String,String]) extends Serializable;
   val customDelimiter = "\n\n";
   
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TEST");
    
    sc = new SparkContext(sparkConf);
    sc.hadoopConfiguration.set("textinputformat.record.delimiter",customDelimiter)

    sqlContext = new SQLContext(sc);
    //readHDFSFile(args(0));
    cutomLogic("C:/Users/KOGENTIX/New folder/Testing/src/main/resources/keyvalue");
  }
  
  def cutomLogic(filePath:String){
    val rdd = sc.textFile(filePath);
    
    val temp = rdd.map{ lines => 
      
      val map:Map[String,String] = Map[String,String]();
      
      val splitLines = lines.split("\\n");
      splitLines.foreach{ line => 
        
        if( line.contains("=")){
          
          val arr = line.split("=");
          map += arr(0) -> arr(1);
          
        }else if( line.trim().length() != 0 ){
          map += "tempDate" -> line;
        }
      }
      
       Iterable(Test(map));
    }
    
    val temps = temp.flatMap(f => f );
    process(sqlContext, temps);
    
  }
 
  def anotherLogic(filePath:String){
    val rdd = sc.textFile(filePath);
    val lines = rdd.filter(x=> x.trim()!="");
    val addDate = lines.map( x => if( x.contains("=") ){x}else{"tempDate"+x} );
    val atualLines = addDate.map( x => (x.split("==")(0),x.split("==")(1)) );
    
    
  }
  
  
  def readHDFSFile(filePath:String){
    
    val hadoopConfig = new Configuration();
    val hdfs = FileSystem.get(hadoopConfig);
    val path = new Path(filePath);
    
    val in:FSDataInputStream = hdfs.open(path);
    
    val list:ListBuffer[Test] = new ListBuffer();
    
    var hashMap:Map[String,String] = Map();
    var key:String = "";
    var value:String = "";
    var line = in.readLine();
    
    while( line != null ){
      
      println("Line ==> ",line);
    	val arr = line.split("=");
    	if( arr.length == 1 ){

    		if( hashMap.size != 0 ){
    			list += new Test(hashMap);
    			hashMap = Map[String,String]();
    		}

    		key = "Date";
    		value = arr(0).trim();
    	}else{
    		key = arr(0).trim();
    		value = arr(1).trim();
    	}

    	if( arr.length > 0 ){
    	  hashMap += key -> value;
    	}
    	
    	line = in.readLine();
    	if( line != null  && line.trim().length() == 0 ){
    	  line = in.readLine();
    	}

    	if( line == null ){
    		list += new Test(hashMap);
    	}
    }

    val rdd = sc.parallelize(list);
    process(sqlContext,rdd);
    
  }
  
  def main1(args: Array[String]): Unit = {
    
    val lines = Source.fromFile("C:/Users/KOGENTIX/New folder/Testing/src/main/resources/keyvalue").getLines().toList;
    val list:ListBuffer[Test] = new ListBuffer();
    
    var hashMap:Map[String,String] = Map();
    var line = "";
    var key:String = "";
    var value:String = "";
    
    println("No of Lines",lines.length);
    
    for( i <- 0 to ( lines.length - 1) ){

    	line = lines(i);
    	println("Line ==> ",line);
    	val arr = line.split("=");
    	if( arr.length == 1 ){

    		if( hashMap.size != 0 ){
    			list += new Test(hashMap);
    			hashMap = Map[String,String]();
    		}

    		key = "Date";
    		value = arr(0).trim();
    	}else{
    		key = arr(0).trim();
    		value = arr(1).trim();
    	}

    	if( arr.length > 0 ){
    	  hashMap += key -> value;
    	}
    	

    	if( i ==  lines.length - 1){
    		list += new Test(hashMap);
    	}
    }
    
    
    println("Total Length",list.length);
    val sparkConf = new SparkConf().setMaster("local").setAppName("TEST");
    sc = new SparkContext(sparkConf);
    val rdd = sc.parallelize(list);
    sqlContext = new SQLContext(sc);
    
    //val df = sqlContext.createDataFrame(rdd,
    process(sqlContext,rdd);
  }
  
  var sc:SparkContext = null;
  var sqlContext:SQLContext = null;
  
  def process( sqlContext:SQLContext,rdd:RDD[Test]){
	  import sqlContext.implicits._;
	  val df = rdd.toDF("map");

	 var dfTemp =  df.withColumn("id", (monotonically_increasing_id()))
	  .select($"id", explode($"map"))
	  .groupBy("id")
	  .pivot("key")
	  .agg(first("value"));
	 
	 dfTemp = dfTemp.drop("id");
	 dfTemp.show();
	 
	sqlContext.udf.register("typeconversion", getValue _);

  }
  
  def getValue(value:String,typeName:String):String={
    if(value.trim().equals("?")){
      var changedValue:String = "";
      if( typeName.contains("double")){
        changedValue = "0.0";
      }else if(typeName.contains("decimal")){
        changedValue = "0.0";
      }else if(typeName.contains("float")){
        changedValue = "0.0";
      }else if(typeName.contains("long")){
        changedValue = "0";
      }else if(typeName.contains("bigint")){
        changedValue = "0";
      }else if(typeName.contains("int")){
        changedValue = "0";
      }
      
      return changedValue;
    }
    
    return "value";
  }
  
}