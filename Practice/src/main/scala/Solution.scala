import scala.language.postfixOps
import scala.util.Random

// you can write to stdout for debugging purposes, e.g.
// println("this is a debug message")

object Solution {

  def main(args: Array[String]): Unit = {
    val sol = solution("query1;filter:id > 10,limit: 10;avg: timestamp");
    println(sol);
  }
  
case class StreamGenerator(seed: String = "Avira - the security company") {
	private[this] val random = new Random(seed.hashCode)
			private[this] def nextId: String = (random.nextInt(10) + 1).toString
			private[this] def nextName: String = random.nextString(7)
			private[this] def nextActionName: String = random.nextInt().abs % 1000 match {
			case 1 => "click"
			case x if x % 5 == 1 => "close"
			case _ => "open"
			}

			private[this] def nextTimestamp: Long = random.nextInt(Int.MaxValue / 2) + 1L
					private[this] def nextAction(prev: Action) = Action(nextId, nextActionName, prev.timestamp + nextTimestamp)

					val stream: Stream[Action] = Stream
					.iterate(Action(nextId, nextActionName, nextTimestamp))(action => nextAction(action))
					.takeWhile(_.timestamp < Long.MaxValue)
}

case class Action(id: String, action: String, timestamp: Long)

object DS {
	val empty: DS = DS(Stream.empty)
}

case class DS(private val actions: Stream[Action]) {
	private[this] def cmp[T](name: String)(f: Action => T)(value: T)(implicit ev: T => Ordered[T]) = name match {
	case "=" => a: Action => f(a) == value
	case "<" => a: Action => f(a) < value
	case ">" => a: Action => f(a) > value
	}

	private[this] def filterOperation(field: String, op: String, value: String) = field match {
	case "id" => cmp(op)(_.id)(value)
	case "action" => cmp(op)(_.action)(value)
	case "timestamp" => cmp(op)(_.timestamp)(value.toLong)
	}

	def filter(query: String): DS = query split ' ' map (_.trim) match {
	case Array(field, op, value) => DS(actions.filter(filterOperation(field, op, value)(_)))
	case _ => DS.empty
	}
	
	def limit(limit: String): DS={
	  println("limit",limit);
	  DS(actions.take(limit.toInt))
	}
	
	def avg(field:String) = field match{
	  case "id" => "undefined"
	  case "action" => "undefined"
	  case "timestamp" => ( actions.map(_.timestamp ).sum / actions.length ).toString()
	}
	
	def max(field:String) = field match{
	  case "id" => actions.map(_.id ).max.toString()
	  case "action" => actions.map(_.action ).max.toString()
	  case "timestamp" => actions.map(_.timestamp ).max.toString()
	}
	
	def count = actions.length.toString()

}

def applyOp(op: String, ds: DS): DS = op split ':' map (_.trim) match {
case Array("filter", query) => ds filter query
case Array("limit", value) => ds limit value
case _ => DS.empty
}

def applyFinalOp(op: String, ds: DS): String = op.trim split ':' map (_.trim) match {
  case x if x(0) == "avg" => ds.avg( x(1) )
  case x if x(0) == "max" => ds.max( x(1) )
  case x if x(0) == "count" => ds.count
  case _ =>  "undefined"
}

def solution(x: String): String = {
		val Array(seed, ops, finalOp) = x split ';' map (_.trim)
		println("ops",ops);
				val operations = ops split ','
				println("operations")
				operations.foreach(println)
				val ds = DS(StreamGenerator(seed).stream)
				println("ds",ds)
				val processedDS = operations.foldLeft(ds)((dataset, query) => applyOp(query, dataset))
				println("finalOp",finalOp)
				println("processedDS",processedDS)
				applyFinalOp(finalOp, processedDS)
}
}