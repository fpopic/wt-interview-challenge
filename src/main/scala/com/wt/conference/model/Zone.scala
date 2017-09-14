package com.wt.conference.model

import org.apache.spark.sql.Row

// 53,17
case class Zone(id: Int, name: String)

object Zone extends Parsable with Serializable {

  def isParsable(parts: Row): Boolean = {
    try {
      assert(parts.length == 2)
      parts.getString(0).toInt
      assert(parts.getString(1).nonEmpty)
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Zone not parsable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }

}
