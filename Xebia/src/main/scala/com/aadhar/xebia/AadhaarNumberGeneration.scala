package com.aadhar.xebia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger
import org.apache.log4j.Level;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.percent_rank;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.broadcast.Broadcast

object AadhaarNumberGeneration {
  
  val sparkSession:SparkSession = SparkSession.builder().master("local")
                                 .appName("AadhaarNumberGeneration").getOrCreate();
  val hiveContext:SQLContext    = sparkSession.sqlContext;
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  val numRecordsShow = 100;
  
  def main(args: Array[String]): Unit = {
    
    val aadharSchama = StructType(Array(StructField("date", DataTypes.DateType, false),
      StructField("registrar", DataTypes.StringType, false),
      StructField("private_agency", DataTypes.StringType, false),
      StructField("state", DataTypes.StringType, false),
      StructField("district", DataTypes.StringType, false),
      StructField("sub_district", DataTypes.StringType, false),
      StructField("pincode", DataTypes.StringType, false),
      StructField("gender", DataTypes.StringType, false),
      StructField("age", DataTypes.IntegerType, false),
      StructField("aadhaar_generated", DataTypes.IntegerType, false),
      StructField("rejected", DataTypes.IntegerType, false),
      StructField("mobile_number", DataTypes.IntegerType, false),
      StructField("email_id", DataTypes.IntegerType, false)));
    
    val df = hiveContext.read.format("com.databricks.spark.csv")
            .options(Map("header" -> "false"))
            .option("dateFormat", "yyyymmdd")
            .schema(aadharSchama)
            .option("delimiter", ",").load("C:/Users/KOGENTIX/New folder/Xebia/src/main/resources/aadhaar_data.csv");
    
    println("6::: ================ Spark DataFrame top 25 rows. ================");
    df.show(25,false);
    df.createOrReplaceTempView("AADHAR_DATA");
    println("Total count in aadhar_data.csv file = "+df.count);
    
    println();
    println("###########################");
    println("##### CHECK POINT 2 #######");
    println("###########################");
    println();
    println("7::: ================ Describe the schema. ================");
    df.printSchema();
    
    println("8::: ================ Count and Names of registrars in aadhar_data.csv file ================");
    val registrar = df.select("registrar").distinct();
    println("Count of registrar :: "+registrar.count());
    registrar.show(numRecordsShow,false);
    
    println("9::: ================ States in aadhar_data.csv file ===============");
    val states = df.select("state").distinct();
    states.show(states.count().toInt,false);
    println("Number of states :: "+states.count());
    
    println("9 ::: ================ District in each state ================ ");
    val distDf = hiveContext.sql("SELECT state,count(district) as numberOfDistricts from AADHAR_DATA Group by state");
    distDf.show(numRecordsShow,false);
    
    println("9 ::: ================ sub-districts in each district ================ ");
    val subDistricts = hiveContext.sql("SELECT state,district,count(sub_district) as numberOfSubdistrictInEachDistrict from AADHAR_DATA GROUP BY state,district ORDER BY state");
    subDistricts.show(numRecordsShow,false);
    
    println("10 ::: ================ number of males and females in each state from the table ================ ");
    val malesFemales = hiveContext.sql(s"""SELECT state, gender, COUNT(gender) FROM AADHAR_DATA GROUP BY state,gender ORDER BY state""".stripMargin);
    malesFemales.show(numRecordsShow,false);
    
    println("11 ::: ================  names of private agencies for each state ================ ");
    val private_agency = hiveContext.sql(s"""SELECT distinct state,private_agency from AADHAR_DATA ORDER BY state""");
    private_agency.show(numRecordsShow,false);
    
    println("12 ::: ================  number of private agencies for each state ================ ");
    val pDf = private_agency.select("state", "private_agency").groupBy("state").count().orderBy(col("count").desc);
    pDf.show(numRecordsShow,false);
    
    
    println("###########################");
    println("##### CHECK POINT 3 #######");
    println("###########################");
    println();
    
    println("13 ::: ================  top 3 states generating most number of Aadhaar cards ================  ");
    val generateAadhar = df.select("state","aadhaar_generated").where(" aadhaar_generated > 0").groupBy("state").count().orderBy(col("count").desc);
    generateAadhar.show(3,false);
    
    println("14 ::: ================  top 3 private agencies generating the most number of Aadhar cards================  ");
    val topPrivateAgencies = df.select("private_agency").groupBy("private_agency").count().orderBy(col("count").desc).limit(3);
    topPrivateAgencies.show(false);
    
    println("15 ::: ================  number of residents providing email, mobile number ================  ");
    println("number of residents providing both email, mobile number ::: "+df.filter("aadhaar_generated > 0 and mobile_number > 0 and email_id > 0").count);
    println("number of residents providing email ::: "+df.filter("aadhaar_generated > 0 and email_id > 0").count);
    println("number of residents providing mobile number ::: "+df.filter("aadhaar_generated > 0 and mobile_number > 0").count);
    println();
    
    println("16 ::: ================  top 3 districts where enrolment numbers are maximum ================ ");
    val top3DistEnrollment = df.select("district","aadhaar_generated").where(" aadhaar_generated > 0").groupBy("district").count().orderBy(col("count").desc);
    top3DistEnrollment.show(3,false);
    
    println("17 ::: ================  no. of Aadhaar cards generated in each state================  ");
    generateAadhar.show(numRecordsShow,false);
    
    println("###########################");
    println("##### CHECK POINT 4 #######");
    println("###########################");
    println();
    
    println("18 ::: ================ Create a data frame using the file and provide its summary. ================");
    println("Please look in AadhaarNumberGeneration.scala class ");
    
    val ageTmpDf = df.select("age").filter("mobile_number > 0").groupBy("age").count().orderBy("age");
    val ageMap = ageTmpDf.collect.map { x => (x.getInt(0),x.getLong(1)) }.toMap;
    map = sparkSession.sparkContext.broadcast(ageMap);
    
    val percentile = udf(getPercentage _);
    
    println("19 ::: ================ command to see the correlation between “age” and “mobile_number ================")
    
    val ageDf = df.select("age").groupBy("age").count().orderBy(col("age").desc);
    val agePercentileDf = ageDf.select(col("age"), percentile(col("age"),col("count")).alias("percentageOfMobileUsed")).orderBy(col("percentageOfMobileUsed").desc);
    agePercentileDf.show(numRecordsShow);
    
    println("20 ::: ================ number of unique pincodes in the data ================");
    val uniquePinCodeCount = df.select("pincode").distinct().count();
    println(uniquePinCodeCount);
    
    println("21 ::: ================ number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra ================ ");
    val rejectedDf = df.filter("rejected > 0 AND state IN ('Uttar Pradesh','Maharashtra')").select("state").groupBy("state").count();
    rejectedDf.show;
    
    println("###########################");
    println("##### CHECK POINT 5 #######");
    println("###########################");
    println();
    
    println("22 ::: ================ The top 3 states where the percentage of Aadhaar cards being generated for males is the highest. ================")
    val maleHighestDf1 = df.filter("gender='M' and aadhaar_generated > 0").select("state", "gender").groupBy("state").count().orderBy(col("count").desc).limit(3);
    maleHighestDf1.show();
    
    println("23 ::: ================ In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest ================ ");
    val top31 = maleHighestDf1.collect().map { x => "'"+x.getString(0)+"'" }.mkString(",");
    println(top31);
    val femaleRejectedDF1 = df.filter(s"rejected > 0 AND state IN($top31) AND gender='F'");
    femaleRejectedDF1.createOrReplaceTempView("TEMP");
    
    val femaletmp = hiveContext.sql(" SELECT * FROM ( SELECT state,district,cnt, row_number() over ( partition by state order by cnt desc) as rownum FROM ( SELECT state,district,count(district) as cnt FROM TEMP group by state,district order by state,district desc)) where rownum <= 3");
    femaletmp.show;
    
    println("24 ::: ================ The top 3 states where the percentage of Aadhaar cards being generated for females is the highest. ================")
    val femaleHighestDf1 = df.filter("gender='F' and aadhaar_generated > 0").select("state", "gender").groupBy("state").count().orderBy(col("count").desc).limit(3);
    femaleHighestDf1.show();
    
    println("25 ::: ================ In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest ================ ");
    val top3 = femaleHighestDf1.collect().map { x => "'"+x.getString(0)+"'" }.mkString(",");
    println(top3);
    val maleRejectedDF1 = df.filter(s"rejected > 0 AND state IN($top3) AND gender='M'");
    maleRejectedDF1.createOrReplaceTempView("TEMP");
    
    val maletmp = hiveContext.sql(" SELECT * FROM ( SELECT state,district,cnt, row_number() over ( partition by state order by cnt desc) as rownum FROM ( SELECT state,district,count(district) as cnt FROM TEMP group by state,district order by state,district desc)) where rownum <= 3");
    maletmp.show;
    
    println("26 ::: ================ summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets. ================ ");
    val maxAge = df.agg(max(col("age"))).collect().map { x => x.getInt(0) }.apply(0);
    val group = math.abs(maxAge.toDouble/10) + 1;
    val bMaxAge = sparkSession.sparkContext.broadcast(math.round(group));
    
    def getGroup(age:Int):String={
      
      val group = bMaxAge.value;
      if( age <= group ){
        return s"0 - $group";
      }else if( age > group  && age <= group * 2){
        return s"$group - ${group*2}";
      }else if( age > group * 2 && age <= group * 3){
        return s"${group* 2} - ${group*3}";
      }else if( age > group* 3 && age <= group * 4){
        return s"${group*3} - ${group*4}";
      }else if( age > group* 4 && age <= group * 5){
        return s"${group*4} - ${group*5}";
      }else if( age > group* 5 && age <= group * 6){
        return s"${group*5} - ${group*6}";
      }else if( age > group* 6 && age <= group * 7){
        return s"${group*6} - ${group*7}";
      }else if( age > group* 7 && age <= group * 8){
        return s"${group*7} - ${group*8}";
      }else if( age > group* 8 && age <= group * 9){
        return s"${group*8} - ${group*9}";
      }else if( age > group* 9 && age <= group * 10){
        return s"${group*9} - ${group*10}";
      }
      
      return "NO GROUP";
    }
    
    val ageGroup = udf(getGroup _);
    val ageGrpDF = df.select(col("age"), ageGroup(col("age")).alias("group"));
    val ageGrpDFTmp = ageGrpDF.select("group").groupBy("group").count().orderBy(col("group").asc);
    ageGrpDFTmp.createOrReplaceTempView("AGE_GRP");
    
    val ageGrpFilterd = df.filter("rejected == 0").select(col("age"), ageGroup(col("age")).alias("filteredgroup"));
    val ageGrpFilterdTmp = ageGrpFilterd.select("filteredgroup").groupBy("filteredgroup").count().orderBy(col("filteredgroup").asc);
    ageGrpFilterdTmp.createOrReplaceTempView("AGE_GRP_FILTERED");
    
    val totalDF = hiveContext.sql("SELECT a.group,concat( bround( b.count* 100/a.count,2 ),' %') as percentage FROM AGE_GRP a JOIN AGE_GRP_FILTERED b on a.group=b.filteredgroup");
    totalDF.show();
  }
  
  def getPercentage(age:Int,mobileNumCount:Long ):String={
    if( map.value.contains(age)){
      val mCount = map.value.get(age).get;
      return mCount*100/mobileNumCount + "%";
    }else{
      return "0%";
    }
  }
  
  var map:Broadcast[Map[Int,Long]] = null;
  
  
  def fromHive(){
    val df = hiveContext.sql("SELECT * FROM aadhaar_managed LIMIT 25");
    df.show();
  }
  
}