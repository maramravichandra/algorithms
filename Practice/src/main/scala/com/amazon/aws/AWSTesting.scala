/*import scala.io.Source
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.parquet._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.{ListObjectsV2Request}
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.{Logger,LogManager,Level,PropertyConfigurator}
import org.apache.logging.log4j.core.lookup.MainMapLookup
import org.apache.spark.SparkContext
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory,JsonValidator}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.report.ProcessingReport
import org.apache.hadoop.log
import java.util.Date
import scala.util.Try
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Calendar
import org.joda.time.DateTime
import java.time._
import java.util.GregorianCalendar;
import org.joda.time.DateTimeZone.UTC
import java.time.LocalDate
object process_arl_data_distilled extends java.io.Serializable {
  val suffix = "/" 
  val spark = SparkSession
                .builder
                .appName("load_distilled_data").master("local")
                .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
                .getOrCreate()
  val context = spark.sparkContext      
  val sqlContext = spark.sqlContext;
  spark.udf.register("getDateOfBirth", getDateOfBirth _)
  spark.udf.register("formatdate", formatdate _)
  spark.udf.register("LOCATE", (s1: String,s2: String) => s2.contains(s1).toString())
 // val logger1 =  LogManager.getLogger();
   // var logger: Logger = LogManager.getLogger(process_agents_data.this)
    //private val log = LogManager.getLogger(this.getClass)
  //val identifier = "arl"
 //System.setProperty("username", identifier)
 //MainMapLookup.setMainArguments("identifier app")
 //PropertyConfigurator.configure("log4j.properties")
 //val configLogProps = Option(getClass.getClassLoader.getResource("log4j.properties"))
//var cus_log = Logger.getLogger("cusLogger")
//cus_log.setLevel(Level.INFO)
  
def initS3Client(): AmazonS3Client = {
   //val credentials = new BasicAWSCredentials("myKey", "mySecretKey");
//val s3Client = new AmazonS3Client(credentials);
    val credential = new BasicAWSCredentials("", "")
    return new AmazonS3Client(credential)
    //return new AmazonS3Client()
}
def getsourcefolder(source_bucket : String,source_bucketfolder: String ,region : String,app : String,source_file : String,source_system : String,loaddate : String): (String, String) = {
  //464816863426us-east-1glbcocuoeoubudlkeat-lak-raw/dev/platform/advisor/arl/agent/source=DSS/load_date=2018-04-25/batch_id=eaddd825-c809-4bfe-8968-2f03c93f3e62
  
  //val src_key =source_bucketfolder+suffix+"region="+region+suffix+"app=\'"+app+"\'"+suffix+"file=\'"+source_file+"\'"+suffix+"source=\'"+source_system+"\'"+suffix+"loaddate=\'"+load_date+"\'"
    val src_key =region.toLowerCase()+suffix+source_bucketfolder+suffix+app+suffix+source_file+suffix+"source="+source_system+suffix+"loaddate="+loaddate
 
  // println(src_key)
    val s3Client = initS3Client()
    val request = new ListObjectsV2Request().withBucketName(source_bucket).withPrefix(src_key)
    
    val result  = s3Client.listObjectsV2(request).getObjectSummaries.asScala.map(_.getKey)
    val result1 = result.toArray
    //println(result)
    val result21 = "dev/platform/advisor/arl/agent/source=TEB/loaddate=2018-05-21/batchid=24deed6d-ba49-43ff-955b-62677343b835/_SUCCESS"//result1.last.split("/")
    val result2 = result21.split("/")
    //println(result2(7))
     var src_folder = ""
     var batch_id = ""
    if(result2(7).length()>0 && result2(7).contains("batchid")){
    // tt-data-landing-arl/platform/region='DEV'/app='arl'/file='agent'/source='TEB'/load_date='2018-04-19'/batch_id='36bc1c7a-02f1-4bfd-8d40-d0d72f78fe51'
      //src_folder=source_bucket+suffix+source_bucketfolder+suffix+"region=\'"+region+"\'"+suffix+"app=\'"+app+"\'"+suffix+"file=\'"+source_file+"\'"+suffix+"source=\'"+source_system+"\'"+suffix+"loaddate=\'"+load_date+"\'/"+batch(6)+suffix+batch(7)
      src_folder=source_bucket+suffix+region.toLowerCase()+suffix+source_bucketfolder+suffix+app+suffix+source_file+suffix+"source="+source_system+suffix+"loaddate="+loaddate+suffix+result2(7)//+suffix+batch(7)
      
      //println("Source Key value1 is: " +src_folder)
      
      batch_id = result2(7)//.toString()
    }
    else {
      //println("Batch id not found: ")
      System.exit(1)
    }
   // println("Source Key value is: " +src_folder)
    return (src_folder, batch_id)
}
def gettargetfolder(target_bucketName : String,target_bucketfolder: String ,region : String,app : String,source_file : String,source_system : String,loaddate : String,batchid : String): String = {
  //Sample output: tt-data-raw-arl/dev/platform/advisor/arl/agent/source=DSS/loaddate=2018-04-19/batchid=4c5d588f-0c42-40e1-8b4b-128354c96785
    //val target_key =target_bucketName+suffix+region.toLowerCase()+suffix+target_bucketfolder+suffix+app+suffix+source_file+suffix+"source="+source_system+suffix+"loaddate="+load_date+suffix+batch_id.replaceAll("'", "")+suffix
    val target_key =target_bucketName+suffix+region.toLowerCase()+suffix+target_bucketfolder+suffix+app+suffix+source_file+suffix+"source="+source_system+suffix+"loaddate="+loaddate+suffix+batchid+suffix
    
    //println("Target Key value is: " +target_key)
    
    return target_key
}
def formatdate(date_input:String): String={
    if(date_input==null || date_input==null.isInstanceOf[String])
    return null
    val sdf_in = new SimpleDateFormat("yyyyMMdd")
    val in_date = sdf_in.parse(date_input)
    //println("FormateDate is.. "+in_date.toString())
   val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val dobDate = sdf.format(in_date);
   //val format = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd");
 //val lDate = org.joda.time.LocalDate.parse(in_date.toString(), format);
     //println("FormateDate1 is.. "+dobDate.toString())
    return dobDate.toString()
   
  }
def validateDob(dateofbirth:String,incoming:String): String={
   val valid="True"
   val invalid="False"
    if(dateofbirth==null || dateofbirth==null.isInstanceOf[String])
    return invalid
    //val sdf_in = new SimpleDateFormat("yyyyMMdd")
    //val in_date = sdf_in.parse(dateofbirth)
    //println(in_date.toString())
   //val sdf = new SimpleDateFormat("yyyy-MM-dd")
  //val dobDate = sdf.parse(in_date.toString());
   val format = org.joda.time.format.DateTimeFormat.forPattern("yyyyMMdd");
 val lDate = org.joda.time.LocalDate.parse(dateofbirth, format);
     //println("DOBDate is.. "+lDate.toDate())
    if(lDate.toString()==incoming)
      return invalid
  val currentDate = new Date();
    //println("currentDate is.. "+currentDate)
    //println("year value for 130 year condition is .. "+ (currentDate.getYear.toInt - lDate.getYear.toInt))
  if(lDate.toDate().getTime > currentDate.getTime ){
    invalid
   }else{
      val years = currentDate.getYear - lDate.getYear;
   // val years = year(col("currentDate")) -  year(col("dobDate"))
   if( years > 130 ) invalid
    else valid
   }
  }
 
   
  def getDateOfBirth(dateofbirth:String,incoming:String):String={
    if ( dateofbirth==null || dateofbirth.equalsIgnoreCase("null") || dateofbirth.trim.length==0) return null
    
      if( Try(dateofbirth.toInt).isFailure || dateofbirth.toInt == 0 ||  dateofbirth.trim().equalsIgnoreCase("00000000")) return null 
 else{
    val sdf = new SimpleDateFormat("yyyyMMdd")
     val date = sdf.parse(dateofbirth)
     val format = org.joda.time.format.DateTimeFormat.forPattern("yyyyMMdd");
     val lDate = org.joda.time.LocalDate.parse(dateofbirth, format);
     val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
 
      val c_dateofbirth=sdf1.format(date)
      val isvalid = validateDob(dateofbirth,incoming)
      println("isvalid value is . "+isvalid)
      if(isvalid=="True") return c_dateofbirth
      else null.asInstanceOf[String].toUpperCase()
  }
  }
 def main(args : Array[String]): Unit = {
    //cus_log.info("inside main method")
   val input1 = spark.read.option("multiline", true).option("mode","PERMISSIVE").json(args(0))//("D://Users//spathak//Downloads//spark_input.json")
   val lookup_data= ("D://Users//spathak//Desktop//arl_reference_tables.csv")//args(1)
    val format = new SimpleDateFormat("y-MM-dd")
    val format1 = new SimpleDateFormat("y-MM-dd hh:mm:ss")
    val loaddate = format.format(Calendar.getInstance().getTime())
    //val loaddate="2018-04-25"
    val batch_start_time = format1.format(Calendar.getInstance().getTime())
    lazy val jsonSchema = JsonLoader.fromResource("/logschema.json") 
   System.setProperty("hadoop.home.dir", "D:\\winutils\\")
   spark.udf.register("validateDob", validateDob _)
   //val conf = new SparkConf().setMaster("local[2]").setAppName("loadagentdata")
   //val sc = new SparkContext(conf)
   //context.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "");
       //context.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "");
      context.hadoopConfiguration.set("fs.s3a.access.key", "");
       context.hadoopConfiguration.set("fs.s3a.secret.key", "");
       //context.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") 
       //context.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      context.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
       
    //lazy val jsonSchema = Option(getClass.getClassLoader.getResource("logschema.json"))
    import spark.implicits._
    
    val pipeline_id = input1.select("pipeline_id").first().getString(0) 
    val app_id = input1.select("app_id").first().getString(0)
    val source_bucket = input1.select("source_bucket").first().getString(0)
    val input_type = input1.select("input_type").first().getString(0)
    val source_file = input1.select("source_file").first().getString(0)
    val target_bucketName = input1.select("target_bucketName").first().getString(0)
    val target_bucketfolder = input1.select("target_bucketfolder").first().getString(0)
    val logbucket = input1.select("logbucket").first().getString(0)
    val source_system = input1.select("source_system").first().getString(0)
    val source_bucketfolder = input1.select("source_bucketfolder").first().getString(0)
    val region = input1.select("region").first().getString(0)
    val query1 = input1.select("query1").first().getString(0)
    val app = app_id.takeRight(3) 
    val output_type = "parquet"
    
    //val bucket_name = "tt-data-landing"
    //source folder = tt-data-landing-arl/platform/region='DEV'/app='arl'/file='agent'/source='TEB'/load_date='2018-04-19'/batch_id='36bc1c7a-02f1-4bfd-8d40-d0d72f78fe51'
   
    //println("load_date is : "+load_date)
    //println("Input file values are: "+input1)
    //println("region values are: "+region)
    //val src_key1 =source_bucketfolder+suffix+"region=\'"+region+"\'"+suffix+"app=\'"+app+"\'"+suffix+"file=\'"+source_file+"\'"+suffix+"source=\'"+source_system+"\'"+suffix+"load_date=\'"+load_date+"\'/"
 
   val (src_key,batchid) = getsourcefolder(source_bucket,source_bucketfolder ,region,app,source_file,source_system,loaddate)
    //val myRDD = context.textFile("s3a://tt-data-landing/platform/region='DEV'/source='HORIZON'/load_date='2018-04-17'/batch_id='873a84d1-4ded-4842-9500-3972c6ef8f56'/file='agent'/hznagents_2018-04-17.txt")
   val  trgt_key = gettargetfolder(target_bucketName,target_bucketfolder,region,app,source_file,source_system,loaddate,batchid)
   println("src_key value is : "+src_key)
   println("trgt_key value is : "+trgt_key)
   //val df = spark.read.parquet("s3a://"+src_key+"/part_*.parquet")
   
   val myRDD = spark.read.format("CSV")
                .option("delimiter","|")
                .option("header","true")
                //.option("inferSchema", "true")
                .load(lookup_data)
                
    
    //println(myRDD.count())
    val lookup = myRDD.toDF()
    lookup.createTempView("arl_standardization")
   
   val df = sqlContext.read.parquet("s3a://"+src_key+"/part-*.parquet")
              //.parquet("s3://"+src_key)
                
    
    //println(df.count())
    
    df.createTempView("data")
   val query1_exec = sqlContext.sql(query1)
   println(query1_exec.show(10))
    val record_count = query1_exec.count().toInt
   query1_exec.createTempView("data1")
    //cus_log.info("record count: " + record_count)
    val column = query1_exec.columns.toArray.mkString("{ \"fields\": [\"", "\",\"", "\"]}")
    //cus_log.info("columns : "+ column)
   
    //cus_log.info(message)
    //row.printSchema()
    //df.write.parquet("D://Users//spathak//Downloads//hznagentsparquet//")
    
    //row.write.parquet("s3a://"+trgt_key)
    //row.write.parquet("s3://"+trgt_key)
    val batch_end_time = format1.format(Calendar.getInstance().getTime())
    //val toRemove = "batch_id=".toSet
    //val upd_batch_id = batch_id.filterNot(toRemove)
    val upd1_batch_id = batchid.replaceAll("batchid=", "")
    val upd_batch_id = upd1_batch_id.replaceAll("'", "")
    val s3_prefix = "https://s3.amazonaws.com/"
    //cus_log.info("{\"app_id\": "+"\""+app_id+"\""+",\"pipeline_id\":"+"\""+pipeline_id+"\""+",\"source_system\":"+"\""+source_system+"\""+",\"load_date\":"+"\""+load_date+"\""+",\"batch_id\":"+"\""+upd_batch_id.replaceAll("'", "")+"\""+",\"batch_start_time\":"+"\""+batch_start_time+"\""+",\"batch_end_time\":"+"\""+batch_end_time+"\""+",\"source_location\":"+"\""+s3_prefix+src_key+"\""+",\"dest_location\":"+"\""+s3_prefix+trgt_key+"\""+",\"input_type\":"+"\""+input_type+"\""+",\"output_type\":"+"\""+output_type+"\""+",\"total_records\":"+"\""+record_count+"\""+",\"status\": \"SUCCESS\",\"log_location\":"+"\""+logbucket+"\""+",\"data_stage\": \"raw\",\"meta_data\":"+column+"}")
    val jsonobj =  "{\"app_id\": "+"\""+app_id+"\""+",\"pipeline_id\":"+"\""+pipeline_id+"\""+",\"source_system\":"+"\""+source_system+"\""+",\"load_date\":"+"\""+loaddate+"\""+",\"batch_id\":"+"\""+upd_batch_id.replaceAll("'", "")+"\""+",\"batch_start_time\":"+"\""+batch_start_time+"\""+",\"batch_end_time\":"+"\""+batch_end_time+"\""+",\"source_location\":"+"\""+s3_prefix+src_key+"\""+",\"dest_location\":"+"\""+s3_prefix+trgt_key+"\""+",\"input_type\":"+"\""+input_type+"\""+",\"output_type\":"+"\""+output_type+"\""+",\"total_records\":"+record_count+",\"status\": \"SUCCESS\",\"log_location\":"+"\""+logbucket+"\""+",\"data_stage\": \"raw\",\"meta_data\":"+column+"}"
   println(jsonobj)
  
    lazy val factory: JsonSchemaFactory = JsonSchemaFactory.byDefault()//getDefault
    val validator: JsonValidator = factory.getValidator
    val schemaJson: com.fasterxml.jackson.databind.JsonNode = asJsonNode(parse(jsonSchema.toString()))
    val instance: com.fasterxml.jackson.databind.JsonNode = asJsonNode(parse(jsonobj))
    println(instance.toString())
    val report: ProcessingReport = validator.validate(schemaJson, instance)
    println(report.toString())
    if (report.isSuccess()){
   // val log_message = context.parallelize(Seq(List("app_id: "+app_id+",pipeline_id:"+pipeline_id+",source_system:"+source_system+",load_date:"+load_date+",batch_id:"+batch_id+",batch_start_time:"+batch_start_time+",batch_end_time:"+batch_end_time+",source_location:"+src_key+",dest_location:"+trgt_key+",input_type:"+input_type+",output_type:"+output_type+",total_records:"+record_count+",status: SUCCESS,log_location:"+logbucket+",data_stage: raw,meta_data:"+column)))
    //val rowRdd = log_message.map(v => Row(v: _*))
    
    //rowRdd.coalesce(1,shuffle = true).saveAsTextFile("s3a://"+logbucket+"/Logs/"+source_system+"/log_"+load_date+".log")
    
    val s3Client = initS3Client()
    val result = s3Client.putObject(logbucket,"Logs/"+source_system+"/raw/log_"+loaddate+".log", jsonobj)//putObject(logbucket, key, input, metadata)
    }else {
      val jsonobj =  "{\"app_id\": "+"\""+app_id+"\""+",\"pipeline_id\":"+"\""+pipeline_id+"\""+",\"source_system\":"+"\""+source_system+"\""+",\"load_date\":"+"\""+loaddate+"\""+",\"batch_id\":"+"\""+upd_batch_id.replaceAll("'", "")+"\""+",\"batch_start_time\":"+"\""+batch_start_time+"\""+",\"batch_end_time\":"+"\""+batch_end_time+"\""+",\"source_location\":"+"\""+s3_prefix+src_key+"\""+",\"dest_location\":"+"\""+s3_prefix+trgt_key+"\""+",\"input_type\":"+"\""+input_type+"\""+",\"output_type\":"+"\""+output_type+"\""+",\"total_records\":"+record_count+",\"status\": \"ERROR\",\"log_location\":"+"\""+logbucket+"\""+",\"data_stage\": \"raw\",\"meta_data\":"+column+",\"Error_Message\":+RuntimeException(report.toString)}"
  
    }
    
    //cus_log.info("app_id: "+app_id+",pipeline_id:"+pipeline_id+",source_system:"+source_system+",load_date:"+load_date+",batch_id:"+batch_id+",batch_start_time:"+batch_start_time+",batch_end_time:"+batch_end_time+",source_location:"+src_key+",dest_location:"+trgt_key+",input_type:"+input_type+",output_type:"+output_type+",total_records:"+record_count+",status: SUCCESS,log_location:"+logbucket+",data_stage: raw,meta_data:"+column)
    
    println("Agent Data Loaded Successfully!!!")
  }
}*/