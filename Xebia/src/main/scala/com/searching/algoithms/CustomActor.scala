package com.searching.algoithms

import akka.actor._
import akka.actor.AbstractActor.Receive
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer


class CustomActor(souceList:ListBuffer[Object], fieldName:String, matchValue:Any, list:ArrayBuffer[Object] ) extends Runnable{
  
 def run(){
   val tempList = souceList.iterator;
   while( tempList.hasNext ){
	   val obj = tempList.next();
	   val fld = obj.getClass.getDeclaredField(fieldName);
	   fld.setAccessible(true);
	   if( fld.get(obj) == matchValue ){
	     list += obj;
	   }
   }
 }
}