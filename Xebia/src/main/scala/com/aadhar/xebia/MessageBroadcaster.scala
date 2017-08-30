package com.aadhar.xebia

import java.util.UUID;
import scala.collection.mutable._;
import util.control.Breaks._;


/**
 * Case class creations
 */
case class Channel(id:UUID,name:String,phoneNumber:String);
case class User(id:UUID,name:String);
case class Following(channelId: UUID, userId: UUID);
case class PhoneNumber(number: String)
  
object MessageBroadcaster {

  val channels: ListBuffer[Channel] = ListBuffer.empty;
  val users: ListBuffer[User] = ListBuffer.empty;
  val followings: ListBuffer[Following] = ListBuffer.empty;
  val phoneNumbers: ListBuffer[PhoneNumber] = ListBuffer.empty;
  val phoneNumbersUsed:Map[String,Int] = Map.empty;

  def main(args: Array[String]): Unit = {
    
    addPhoneNumbers(3);
    addChannels(10);
    addUsers(20);
    addFollowings();
    
    addNewChannel();
  }
  
  /**
   * Adds Phone Number information
   */
  def addPhoneNumbers(numPhoneNumbers:Int){
    val number:Long = 9160811477L;
    
    for( phone <- 1 to numPhoneNumbers ){
      val phoneNumber = new PhoneNumber(s"+91${number+phone}");
      phoneNumbers += phoneNumber;
      
      phoneNumbersUsed += phoneNumber.number -> 0;
    }
  }
  
  /**
   * Adds Channel information
   * To use existing phone numbers in channel i have used below formula 
   * ( math.max(math.round( phoneNumbers.length/i),1) - 1 ).toInt. It will give phone number index for each channel.
   */
  def addChannels(numChannel:Int){
    
    for( i <- 1 to numChannel ){
      addNewChannel();
    }
  }
  
  /**
   * Adds user information
   */
  def addUsers(numUsers:Int){
    
    for( i <- 1 to numUsers ){
      val user = new User( UUID.randomUUID(), s"USER-$i");
      users += user;
    }
  }
  
  /**
   * Adds following information
   */
  def addFollowings(){
    
    for( i <- 1 to channels.length ){
      val index = ( math.max(math.round( users.length/i),1) - 1 ).toInt;
      val follower = new Following( channels(i-1).id, users(index).id);
      followings += follower;
    }
    
    //Assigning channels to the user who's user is not following the channel.
    for( i <- 1 to users.length ){
    	if( !isUserFollowingChannel(users(i-1).id) ){
    		val index = ( math.max(math.round( channels.length/i),1) - 1 ).toInt;
    		val follower = new Following( channels(index).id, users(i-1).id);
    		followings += follower;
    	}
    }
    
    //Assigning multiple channels to the user without allowing collision 
    for( i <- 1 to followings.length ){
      val following = followings(i-1);
      
      for( j <- 1 to channels.length ){
         if( !isCollision(following.userId, channels(j-1).id) ){
            val follower = new Following( channels(j-1).id, following.userId);
    		    followings += follower;
         }
       }
    }
  }
  
  /**
   * verfies channel is followinig channel or not
   */
  def isUserFollowingChannel(userId:UUID ):Boolean={
    val following = followings.filter { x => x.userId == userId };
    return following.length > 0;
  }
  
  /**
   * Verifies the collision happens with channel or not
   */
  def isCollision( userId:UUID, channelId:UUID ):Boolean={
    
    val flwings = followings.filter { x => x.userId == userId };
    val channel = channels.filter { x => x.id == channelId  }(0);
    if( flwings.length > 0 ){
      for( following <- flwings ){
        val tmpChannel = channels.filter { x => x.id == following.channelId  }(0);
        if( tmpChannel.phoneNumber == channel.phoneNumber ){
          return true;
        }
      }
    }
    
    return false;
  }
  
  /**
   * Adds New Channel
   */
  def addNewChannel(){
    val number = getLeastUsedPhoneNumber();
    val channel = new Channel(UUID.randomUUID(),s"Channel-${channels.length+1}",number);
    channels += channel;
    
    var num = 0;
    if( phoneNumbersUsed.contains(number)){
      num = phoneNumbersUsed.get(number).get;
    }
      
    phoneNumbersUsed += number -> ( num + 1);
  }
  
  /**
   * Gets least used phone number
   */
  def getLeastUsedPhoneNumber():String={
    val lest = phoneNumbersUsed.values.min;
    val keys = phoneNumbersUsed.find(_._2 == lest).get;
    return keys._1;
  }
}