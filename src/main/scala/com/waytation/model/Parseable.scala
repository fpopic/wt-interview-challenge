package com.waytation.model

trait Parseable {

  def isParsable(parts: Array[String]): Boolean

}