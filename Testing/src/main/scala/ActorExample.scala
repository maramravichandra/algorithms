import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props


class MyActor extends Actor{
  
  def receive = {
    case "Ravi" => println("It's Ravi");
    case "Teja" => println("It's Teja");
    case "Naga" => println("It's Naga");
    case whoa => println("It's ",whoa);
  }
}

object ActorExample {
  
	def main(args: Array[String]): Unit = {
			val system = ActorSystem("My-First-Actor-Example");
			val props = Props[MyActor];
			val actorExample = system.actorOf(props,"MY-ACTOR-EXAMPLE");
			actorExample ! "Ravi";
			actorExample ! "Naga";
			actorExample ! "Teja";
			actorExample ! "Nobady";
			
			val props1 = Props[MyActor];
			val actorExample1 = system.actorOf(props1,"MY-ACTOR-EXAMPLE1");
			actorExample1 ! "Ravi1";
			actorExample1 ! "Naga1";
			actorExample1 ! "Teja1";
			actorExample1 ! "Nobady1";
	}
  
}