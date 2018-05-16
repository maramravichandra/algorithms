package com.collections.practice

import scala.io.Source
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.File

object FilesFunctionality {
  
  def main(args: Array[String]): Unit = {
    
    val os:OutputStream = new FileOutputStream(new File("D:\\env\\example.txt"));;

    val lines = Source.fromFile("D:\\env\\env.conf.1.6.txt").getLines()
    while( lines.hasNext ){
      val line = lines.next() + "\n"
      os.write(line.getBytes(), 0, line.length());
    }
    
    println("File has been written in location")
    os.close()
    
  }
}