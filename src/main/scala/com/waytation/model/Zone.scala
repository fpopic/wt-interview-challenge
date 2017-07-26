package com.waytation.model

// 53,17
case class Zone(id: Int, name: String)

object Zone extends Parseable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      parts(0).toInt
      assert(parts(1).nonEmpty)
    }
    catch {
      case (_: AssertionError) =>
        println(s"Zone name empty: $parts")
        return false
      case (_: NumberFormatException) =>
        println(s"Zone not parsable: $parts")
        return false
    }
    true
  }

}
