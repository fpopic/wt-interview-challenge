package com.waytation.model

// 53,17
case class Zone(id: Int, name: String)

object Zone extends Parsable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      assert(parts.length == 2)
      parts(0).toInt
      assert(parts(1).nonEmpty)
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Zone not parsable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }

}
