package com.waytation.conference.model

import java.sql.Timestamp

import com.waytation.conference.misc.UnixTimestamp._
import org.apache.spark.sql.Row

// 1888,1465658885000,1465906450000
case class Tag(id: Int, activeFrom: Timestamp, activeTo: Timestamp)

object Tag extends Parsable with Considerable with Serializable {

  def isParsable(row: Row): Boolean = {
    try {
      assert(row.length == 3)
      row.getString(0).toInt
      row.getString(1).toLong
      row.getString(2).toLong
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        //println(s"Tag not parsable: ${row.mkString("(", ",", ")")}")
        return false
    }
    true
  }

  def isConsiderable(row: Row): Boolean = {
    try {
      val start = new Timestamp(row.getString(1).toLong)
      val end = new Timestamp(row.getString(2).toLong)
      assert(isValidTimestamp(start))
      assert(isValidTimestamp(end))
      assert(start before end)
    }
    catch {
      case (_: AssertionError) =>
        //println(s"Tag not considerable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }
}