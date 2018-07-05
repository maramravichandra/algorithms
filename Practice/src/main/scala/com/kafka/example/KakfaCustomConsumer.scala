package com.kafka.example

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import java.util.Collections


object KakfaCustomConsumer {
  
  def main(args: Array[String]): Unit = {
    
    val properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "25000");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    
    val topicName = "Ravi";
    
    val consumer = new KafkaConsumer[String,String](properties);
    consumer.subscribe(Collections.singletonList(topicName))
    
    while(true){
      
      val records = consumer.poll(1000);
      for( record <- records ){
        println(s"Kafka Custom Consumer :: Key = ${record.key()}  and value = ${record.value()}")
      }
    }
    
    consumer.close()
    
  }
}