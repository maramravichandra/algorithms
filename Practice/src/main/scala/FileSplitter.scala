import java.io.FileInputStream
import java.io.File
import java.io.BufferedInputStream
import java.io.FileOutputStream


object FileSplitter {
  
  def main(args: Array[String]): Unit = {
    splitFile("D:\\spark\\spark performance.docx", 1024,"docx")
  }
  
  private def splitFile(path:String,bytes:Int,extension:String)={
   
    var partCount = 1;
    val sizeOfTheFile = bytes*bytes;
    val buffer = new Array[Byte](sizeOfTheFile);
   
    val file = new File(path);
    val fileName = file.getName.replaceAll(s".$extension", "")
    try{
      
      val fis = new FileInputStream( file );
      val bis = new BufferedInputStream(fis);
      
      var bytesAmount = 0;
      bytesAmount = bis.read(buffer)
      
      while( bytesAmount > 0 ){
        
        val filePartName = s"${fileName}_$partCount.$extension";
        val newFile = new File( file.getParent, filePartName);
        try{
          val out = new FileOutputStream(newFile)
          out.write(buffer, 0, bytesAmount);
        }catch{
          case e:Exception =>
          e.printStackTrace();
        }
        partCount += 1;
        bytesAmount = bis.read(buffer)
      }
    }catch{
      case e:Exception =>
        e.printStackTrace();
    }
  }
}