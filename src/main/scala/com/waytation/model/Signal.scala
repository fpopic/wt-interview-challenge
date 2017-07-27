package com.waytation.model

import java.sql.Timestamp

import com.waytation.misc.UnixTimestamp._

/*
  The RSSI, a negative number, is an indirect measure for the proximity of a tag to a station.
  E.g.: Tag 101 is scanned by two different stations at 13:00:00.
  Station 1 receives a signal with a RSSI of -60, station 2 one with a RSSI of -70.
  It stands to reason that tag 101 was closer to station 1 than to station 2.
*/

// 1465992149744,-91,53,2027
case class Signal(timestamp: Timestamp, rssi: Int, stationId: Int, tagId: Int)

object Signal extends Parsable with Considerable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      assert(parts.length == 4)
      parts(0).toLong
      parts(1).toInt
      parts(2).toInt
      parts(3).toInt
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        println(s"Signal not parsable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }

  def isConsiderable(parts: Array[String]): Boolean = {
    try {
      val timestamp = new Timestamp(parts(0).toLong)
      assert(isValid(timestamp))
      val hour = timestamp.toLocalDateTime.getHour
      assert(hour >= 10 && hour <= 16)
      val rssi = parts(1).toInt
      assert(rssi >= -100 && rssi <= 0)
    }
    catch {
      case (_: AssertionError) =>
//        println(s"Signal not considerable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }
}