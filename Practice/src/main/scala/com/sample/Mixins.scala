package com.sample

trait A {
  val message = ""
}

class B extends A{
  override val message = "Current Class B extends A"
}

trait C extends A{
  def upperMessage = message.toUpperCase()
}

trait Simple{
  val lowerMessage = "Simple message"
}

class D extends B with C with Simple





