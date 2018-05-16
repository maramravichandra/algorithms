package com.companion.objects

import scala.io.Source

class LazyVal {
  
  lazy val lazyfile = Source.fromFile("D:\\env\\env1.conf.1.6.txt")
  //val file = Source.fromFile("D:\\env\\env1.conf.1.6.txt")
}