package com.kafka.example

import java.util.Properties
import java.util.Scanner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.Random

object KafkaCustomProducer {
  
  def main(args: Array[String]): Unit = {
    
    val random = new Random();
    val topicName = "Ravi"
    val scanner = new Scanner(System.in);
    println("Enter Message and enter exit to quit.")
    
    val properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    
    val producer = new KafkaProducer[String,String](properties)
    var line = scanner.nextLine();
    println(s"Message :: $line")
    while( !line.equalsIgnoreCase("exist")){
      
      val record = new ProducerRecord(topicName,s"${random.nextInt(255)}",line);
      producer.send(record);
      line = scanner.nextLine();
      println(s"Message :: $line")
    }
    
    scanner.close();
    producer.close();
    
  }
}