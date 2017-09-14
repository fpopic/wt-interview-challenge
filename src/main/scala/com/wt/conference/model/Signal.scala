package com.wt.conference.model

import java.sql.Timestamp

import com.wt.conference.misc.UnixTimestamp._
import org.apache.spark.sql.Row

/*
  The RSSI, a negative number, is an indirect measure for the proximity of a tag to a station.
  E.g.: Tag 101 is scanned by two different stations at 13:00:00.
  Station 1 receives a signal with a RSSI of -60, station 2 one with a RSSI of -70.
  It stands to reason that tag 101 was closer to station 1 than to station 2.
*/

// 1465992149744,-91,53,2027
case class Signal(timestamp: Timestamp, rssi: Int, stationId: Int, tagId: Int)

object Signal extends Parsable with Considerable with Serializable {

  def isParsable(row: Row): Boolean = {
    try {
      assert(row.length == 4)
      row.getString(0).toLong
      row.getString(1).toInt
      row.getString(2).toInt
      row.getString(3).toInt
    }
    catch {
      case (_: NumberFormatException | _: AssertionError) =>
        //println(s"Signal not parsable: ${row.mkString("(", ",", ")")}")
        return false
    }
    true
  }

  def isConsiderable(row: Row): Boolean = {
    try {
      val timestamp = new Timestamp(row.getString(0).toLong)
      assert(isValidTimestamp(timestamp))
      val hour = timestamp.toLocalDateTime.getHour
      assert(hour >= 10 && hour <= 16)
      val rssi = row.getString(1).toInt
      assert(rssi >= -100 && rssi <= 0)
    }
    catch {
      case (_: AssertionError) =>
        //println(s"Signal not considerable: ${parts.mkString("(", ",", ")")}")
        return false
    }
    true
  }
}