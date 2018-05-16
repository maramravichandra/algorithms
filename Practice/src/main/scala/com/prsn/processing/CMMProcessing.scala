package com.prsn.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType

object CMMProcessing {
  
  val spark = SparkSession.builder().appName("CMMProcessing").getOrCreate();
  
  def main(args: Array[String]): Unit = {
    
    val prsnSchema = new StructType().add("CRT_DT", TimestampType)
                 .add("LAST_UPD_DT", TimestampType)
                 .add("SRC_SYS_CRT_DT", TimestampType)
                 .add("SRC_SYS_UPD_DT", TimestampType)
                 .add("PRSN_ROW_ID", StringType)
                 .add("MBR_ROW_ID", StringType)
                 .add("ID_TYP_CD", StringType)
                 .add("ID_CMPNT_1", StringType)
                 .add("ID_CMPNT_2", StringType)
                 .add("ID_CMPNT_3", StringType)
                 .add("ID_CMPNT_4", StringType)
                 .add("ID_CMPNT_5", StringType)
                 .add("SUSPECT_ID_FLAG", StringType)
                 .add("EFF_FROM_DT", TimestampType)
                 .add("EFF_THRU_DT", TimestampType)
                 .add("REC_STAT_CD", StringType)
                 
   val prsnAllDF =  spark.read.format("com.databricks.spark.csv").option("delimeter","|")
    .schema(prsnSchema).option("header","true")
    .load("hdfs path of prsn_all");
   prsnAllDF.show();
   
   val memberSchma = new StructType().add("FROM_ENTRP_PRSN_ID", StringType)
                     .add("TO_ENTRP_PRSN_ID", StringType)
                     .add("ENTRP_MBR_ID", StringType)
                     .add("CHG_ACTN_CD", StringType)
                     .add("CHG_ACTN_DT", TimestampType)
   
   val changeDF = spark.read.format("com.databricks.spark.csv").option("delimeter","|")
    .schema(memberSchma).option("header","true")
    .load("hdfs change file path");
   
   
  }
}