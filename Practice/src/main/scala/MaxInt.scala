
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.storage.StorageLevel

object MaxInt {
  
  def main( args:Array[String]){
    
    val conf = new SparkConf
    conf.setMaster("local[*]")
    conf.setAppName("MAX INT")
    val sc = new SparkContext(conf);
    
    val data = sc.textFile("C:\\Users\\KOGENTIX\\Desktop\\maxint.txt", 10);
    val rdd = data.map( value => NumberUtils.toInt(value));
    
    val mapPartRdd = rdd.mapPartitions{ iter =>
      val maxVal = iter.max;
      Iterator(maxVal);
    }
    
    mapPartRdd.persist(StorageLevel.MEMORY_AND_DISK);
    mapPartRdd.count();
    
    val localIterator = mapPartRdd.toLocalIterator;
    var maxValue = 0;
    
    while( localIterator.hasNext ){
      val value = localIterator.next()
      if( value > maxValue ) maxValue = value;
    }
    
    println("Max Value is",maxValue);
    
    val name = "Ravi";
    
    name match {
      
      case "Ravi" =>
        println("YES");
      case "Anil" =>
      println("NO");
      
    }
    
    
    
    
    
    
    
  }
}