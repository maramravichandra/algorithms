package akshay

import org.apache.log4j.Logger
import org.apache.log4j.Level;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._;

object XMLProcessing {
  
  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    import spark.implicits._;
    
    var df = spark.read.format("com.databricks.spark.xml")
    .option("rowTag", "Worksheets")
    .load("D:/L21_HO_NotAPolicy_0021447600_pc-255517_20171218120451.xml");
    
    df = df.withColumn("Worksheet", explode($"Worksheet"))
    
    df = df.select("*")
    df.printSchema();
    df.show(false)
    val dfTmp = df.toJSON;
    dfTmp.show(false)
    dfTmp.createOrReplaceTempView("XML_TEMP_TABLE")
    
    //get_json_object(msg_data,'$.seat_id') as seat_id
    
    val finalDF = spark.sql("""SELECT 'Worksheet' as Worksheet_temp,
                             |  get_json_object(value,'$.Worksheet.FixedId') as _FixedId,
                             |  get_json_object(value,'$.Worksheet._ExpirationDate') as _ExpirationDate,
                             |  get_json_object(value,'$.Worksheet._EffectiveDate') as _EffectiveDate,
                             |  get_json_object(value,'$.Worksheet._Description') as _Description,
                             |  get_json_object(value,'$.Worksheet.Routine._RoutineVersion') as _RoutineVersion,
                             |  get_json_object(value,'$.Worksheet.Routine._RoutineCode') as _RoutineCode,
                             |  get_json_object(value,'$.Worksheet.Routine._RateBookEdition') as _RateBookEdition,
                             |  get_json_object(value,'$.Worksheet.Routine._RateBookCode') as _RateBookCode,
                             |  get_json_object(value,'$.Worksheet.Routine.Store._Variable') as _Variable,
                             |  get_json_object(value,'$.Worksheet.Routine.Store._ResultType') as _ResultType,
                             |  get_json_object(value,'$.Worksheet.Routine.Store._Result') as _Result,
                             |  get_json_object(value,'$.Worksheet.Routine.Store._Declaration') as _Declaration
                             |  
                             |   FROM XML_TEMP_TABLE""".stripMargin)
      finalDF.show()

    
  }
  
}