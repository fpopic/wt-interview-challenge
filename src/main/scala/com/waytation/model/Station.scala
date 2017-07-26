package com.waytation.model

// 17,Room 1
case class Station(id: Int, zoneId: Int)

object Station extends Parseable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      parts(0).toInt
      parts(1).toInt
    }
    catch {
      case (_: NumberFormatException) =>
        println(s"Station not parsable: $parts")
        return false
    }
    true
  }

}
