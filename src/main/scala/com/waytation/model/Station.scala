package com.waytation.model

// 17,Room 1
case class Station(id: Int, zoneId: Int)

object Station extends Parsable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      assert(parts.length == 2)
      parts(0).toInt
      parts(1).toInt
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Station not parsable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }

}
