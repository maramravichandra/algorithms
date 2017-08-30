

object UnApply {
  
  def apply(a:Int):Int={
    return a*10;
  }
  
  def unApply(a:Int):Int={
    if (a %2 == 0) apply(a/2) else 0
  }
}