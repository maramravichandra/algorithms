

import java.util.Properties
import java.io.FileReader
import java.io.File

class CustomProperties {
  private var properties:Properties = null;
  
  def readProperites(propertyFilePath:String){
    properties = new Properties();
    try{
      val file:File = new File(propertyFilePath);
      val reader    = new FileReader(file);
      properties.load(reader);
    }catch{
      case e:Exception =>
        println(e.printStackTrace());
    }
  }
  
  def setProperty(key:String,value:String){
    this.properties.setProperty(key, value);
  }
  
  def getProperty( property:String):String={
    try{
      return properties.getProperty(property);
    }catch{
      case ex:Exception =>{
        println("NO Property existed");
        return "";
      }
    }
  }
  
  def getPropeties():Properties={
    return this.properties;
  }

}