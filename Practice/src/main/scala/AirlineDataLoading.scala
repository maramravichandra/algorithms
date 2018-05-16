import java.io.File

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util

import org.json.JSONObject
import org.apache.spark.SparkConf

import scala.io.Source


object AirlineDataLoading {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.set("hive.exec.dynamic.partition", "true");
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict");

    val spark = SparkSession.builder().master("local").getOrCreate();

    val schema = DataTypes.createStructType(Array(
      DataTypes.createStructField("flight_id", DataTypes.StringType, false),
      DataTypes.createStructField("file_name", DataTypes.StringType, false),
      DataTypes.createStructField("time_stamp1", DataTypes.StringType, false),
      DataTypes.createStructField("host_name", DataTypes.StringType, false),
      DataTypes.createStructField("process_name", DataTypes.StringType, false),
      DataTypes.createStructField("p_id", DataTypes.LongType, false),
      DataTypes.createStructField("t_id", DataTypes.LongType, false),
      DataTypes.createStructField("log_level", DataTypes.IntegerType, false),
      DataTypes.createStructField("version", DataTypes.FloatType, false),
      DataTypes.createStructField("msg_type", DataTypes.StringType, false),
      DataTypes.createStructField("msg_data", DataTypes.StringType, false),
      DataTypes.createStructField("airline", DataTypes.StringType, false),
      DataTypes.createStructField("year", DataTypes.StringType, false),
      DataTypes.createStructField("month", DataTypes.StringType, false),
      DataTypes.createStructField("day", DataTypes.StringType, false)));


    val folder = new File("D:\\SUPPORT\\Teja\\SeatSessionLogs");
    if( folder.isDirectory ){

      var df:Dataset[Row] = null;
      val listOfFiles = folder.listFiles();
      listOfFiles.foreach{ file =>

        var flightId:String = "xxx";
        var file_name:String = "ddd";
        var airline:String = "aaa";
        var year:String = "ddd";
        var month:String="qqq";
        var day:String = "eee";
        var fileName:String = file.getPath;

        println(fileName);
        fileName = fileName.substring(fileName.lastIndexOf("/")+1, fileName.length());

        val rdd = Source.fromFile(file.getPath).getLines();
        val arr = rdd.filter { x => x.trim().length() > 0 }.map{ line =>
          line.replace("\"\"", "\"").split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        }.toList

        println("List Size",arr.length);
        
        arr.foreach{ vlaues =>
          var value = vlaues(8);
          value = value.substring(1,value.length-1);
          val json = new JSONObject(s"$value");
          if( json.has("flight_id")  ){
            flightId = json.get("flight_id").toString;
            println(json);
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val date = sdf.parse(vlaues(0));
            val dt = LocalDate.parse(sdf.format(date));
            println("date",date)
            airline = vlaues(1);
            year = s"${dt.getYear}";
            month = dt.getMonth.toString;
            day = dt.getDayOfMonth.toString;
          }
        }

        var bcvlaues = (flightId,fileName,airline,year,month,day);
        println(bcvlaues)

        println("List Size",arr.length);
        var list:util.ArrayList[Row] = new util.ArrayList[Row]();
        arr.foreach{ vlaues =>
          val sdf = new SimpleDateFormat("yyyy-MM-dd")
          val date = sdf.parse(vlaues(0));
          list.add(Row(bcvlaues._1,bcvlaues._2,sdf.format(date),vlaues(1),vlaues(2),vlaues(3).toLong,vlaues(4).toLong,vlaues(5).toInt,vlaues(6).toFloat,vlaues(7),vlaues(8),bcvlaues._3,bcvlaues._4,bcvlaues._5,bcvlaues._6))
        }

        println("List Size",list.size());
        import spark.implicits._;

        var dfTemp = spark.sqlContext.createDataFrame(list, schema);

        dfTemp = dfTemp.withColumn("time_stamp", $"time_stamp1".cast("timestamp"));
        dfTemp = dfTemp.drop("time_stamp1");
        dfTemp.show(false)
        if( df == null ){
          df = dfTemp;
        }else{
          df = df.union(dfTemp);
        }
      }

      df.show(false);
      //df.createOrReplaceTempView("Temp_Table");
      df.write.mode(SaveMode.Append).partitionBy("airline","year","month","day").csv("/tmp/logs/SestSessionLogs");
      //SET hive.exec.dynamic.partition = true;
      //SET hive.exec.dynamic.partition.mode = nonstrict;
      //spark.sql(s"""INSERT INTO logs_tbls.stvplus_seatsession partition(airline,year,month,day) SELECT flight_id,
        //           | file_name, time_stamp,host_name, process_name, p_id , t_id , log_level , version , msg_type,
          //         | msg_data ,airline,year,month,day FROM Temp_Table""".stripMargin);

    }else{

    }

  }

}
