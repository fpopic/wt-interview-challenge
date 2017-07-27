package com.waytation.model

import java.sql.Timestamp

import com.waytation.misc.UnixTimestamp._

// 1888,1465658885000,1465906450000
case class Tag(id: Int, activeFrom: Timestamp, activeTo: Timestamp)

object Tag extends Parsable with Considerable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      assert(parts.length == 3)
      parts(0).toInt
      parts(1).toLong
      parts(2).toLong
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Tag not parsable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }

  def isConsiderable(parts: Array[String]): Boolean = {
    try {
      val start = new Timestamp(parts(1).toLong)
      val end = new Timestamp(parts(2).toLong)
      assert(isValid(start))
      assert(isValid(end))
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