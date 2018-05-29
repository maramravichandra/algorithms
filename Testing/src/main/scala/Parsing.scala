
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;


object Parsing {
  
  def main(args: Array[String]): Unit = {
    val formatter = new SimpleDateFormat("EE, dd MMMM yyyy hh:mm:ss.SSS ");
    val today = formatter.format(new Date());
    System.out.println("Today : " + today);
  }
  
}