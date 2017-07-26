package com.waytation.model

// 1888,1465658885000,1465906450000
case class Tag(id: Int, activeForm: Long, aciveTo: Long)

object Tag extends Parseable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      parts(0).toInt
      parts(1).toLong
      parts(2).toLong
    }
    catch {
      case (_: NumberFormatException) =>
        println(s"Tag not parsable: $parts")
        return false
    }
    true
  }

}
