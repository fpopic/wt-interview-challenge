package com.wt.conference.model

import org.apache.spark.sql.Row

// 17,Room 1
case class Station(id: Int, zoneId: Int)

object Station extends Parsable with Serializable {

  def isParsable(row: Row): Boolean = {
    try {
      assert(row.length == 2)
      row.getString(0).toInt
      row.getString(1).toInt
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Station not parsable: ${row.mkString("(", ",", ")")}")
        return false
    }
    true
  }

}
