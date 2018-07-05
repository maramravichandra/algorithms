package com.akka.example

import akka.actor.ActorSystem
import akka.actor.Props
import akka.util.Timeout
import java.lang.Long
import scala.concurrent.duration._  
import scala.concurrent.Await
import akka.pattern.ask 
import scala.io.Source

object AkkaExample {
  
  def main(args: Array[String]): Unit = {
    
    var actorSystem = ActorSystem("AkkaExample");
    
    implicit val timeout = Timeout(2000 seconds)
    
    for( i <- 1 to 10000 ){
      val actor1 = actorSystem.actorOf(Props[AkkaActor], "AkkaActor"+i);
      val future = actor1 ? (i*100);
      val result = Await.result(future, timeout.duration);  
      actorSystem.stop(actor1);
    }
    
  }
}