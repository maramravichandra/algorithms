import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object WordCount {
  
  type customType = (Int,Int);
  
  def filter( n:Int, b:Int=>Boolean){
    
    println(n);
    println(b(5));
    println(b(10));
  }
  
  def isEven(x:Int)(y:Int) = ( x%y == 0)
  
	def main(args: Array[String]): Unit = {

	  filter(10, isEven(15));
	  filter(10,isEven(10));
	  
	  val map:Map[String,String] = Map();
	  map ++ "ravi" -> "Ravi";
	  
	  return;
			val conf = new SparkConf().setMaster("local").setAppName("wordCount");
			val sc = new SparkContext(conf);

			val input =  sc.textFile("C:/Users/KOGENTIX/New folder/Testing/src/main/resources/inputfile.txt")

//			val words = input.flatMap(line => line.split(" "));
//			
//			println("============== words ====================");
//			println(words.collect());
//			println("============== words ====================");
//			
//			
//			val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
//			println("============== counts ====================");
//			counts.collectAsMap().foreach( println )
//			println("============== counts ====================");
			
			println("========== GROUPBY KEY ==================");
			val temps = sc.parallelize(Array( Array("Ravi","Ravi","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja"),Array("Ravi","Ravi","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja")));
			val flats = temps.flatMap { x => x };
			val words1 = flats.map { x => (x,1)}.groupByKey().map( t => (t._1,t._2.sum))
			words1.collectAsMap().foreach(println);
			//flats
			
//			val wordTmps = sc.parallelize("qwerty");
//			val temp1s = wordTmps.flatMap { x => x };
//			val maps = temp1s.map { x => (x,1)}

			
			println("========== COMBINEBY KEY ==================");
			val words = sc.parallelize(Array("Ravi","Ravi","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja","Teja"),2);
			val mappedWords = words.map(word => (word, 1));
			val test = mappedWords.combineByKey(createPair, combinePair, mergePair);
			test.collectAsMap().foreach( println );
			val temp = test.map( x => (x._1,x._2._2));
			temp.collectAsMap().foreach( println );
			
			
			println("========== REDUCEBY KEY ==================");
			val test_rbk = words.map(word => (word,1)).reduceByKey(_+_);
			test_rbk.collectAsMap().foreach(println);
	}
	
	def createPair(value:Int):customType={
	  return (1,value);
	}
	
	def combinePair( pair:customType,value:Int):customType={
	  return ( pair._1,pair._2+value);
	}
	
	def mergePair( pair1:customType,pair2:customType ):customType={
	  return ( pair1._1+pair2._1, pair1._2+pair2._2);
	}
	
	def groupBy(key:String,values:Iterable[Int]):(String,Int)={
	  return (key,values.size);
	}
	
	def aggregate(values:Iterable[Int]):Int={
	  return values.size;
	}
	
}