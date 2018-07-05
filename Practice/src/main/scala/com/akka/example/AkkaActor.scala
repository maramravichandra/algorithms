package com.akka.example

import akka.actor.Actor
import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
class AkkaActor extends Actor{
  
  def receive = {
    case value => {
      println(s"""${self.path.name} called .....""")
      for( i <- 0 to 20 ){
        
      }
      println( s"""${self.path.name}, value: $value """)
    }
    
    val senderName = sender()
    senderName ! s"${self.path.name} got message !!!"
  }
  
  private def implementation()={
    
  }
  
  override def postStop()={
    println(s"postStop() called for ${self.path.name}")
  }
}