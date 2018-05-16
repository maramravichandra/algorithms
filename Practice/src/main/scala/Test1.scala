import scala.util.Try
import java.text.SimpleDateFormat


object Test1 {
   def main( args:Array[String] ){
    
     
     val sdf = new SimpleDateFormat("yyyy-mm-dd");
			val date = sdf.parse("2017-08-01 00:00:00.0");
			println(date.getTime);
     
     return;
    val preffix = Array("3","*34","+1*23","+12");
    val ints = Array('1','2','3','4','5','6','7','8','9','0');
    
    val ptemp = preffix.map{ item =>
      
      if( item.length() == 1 )
        "";
      else{
        
        var value = item;
        var infix = "";
        var specialChars = ""
        var i = 0;
        
        while( i < value.length() ){
        	val chr = value(i);
        	if( ints.indexOf(chr) == -1 ){
        		specialChars += chr;
        		i = i+1
        	}else{
        		if( specialChars.length() > 0 ){
        			infix += value(i);
        			value = infix + specialChars + value.substring(infix.length()+specialChars.length(), value.length())
        			i = infix.length()
        			specialChars = "";
        		}else{
        			infix += value(i);
        			i = i+1;
        		}
        	}
        }
        
       println(value)
       
       specialChars = "";
       infix = "";
       
        while( i < value.length() ){
        	val chr = value(i);
        	if( ints.indexOf(chr) == -1 ){
        		specialChars += chr;
        		i = i+1
        	}else{
        		if( specialChars.length() > 0 ){
        			infix += value(i)+specialChars;
        			specialChars = "";
        			value = infix + value.substring(infix.length(), value.length());
        			if( i == 0) i = 0;
        			else i = i -1;
        		}else{
        			infix += value(i);
        			i = i+1;
        		}
        	}
        }
        
       println(value)
        
      }
    }
    
  }
}