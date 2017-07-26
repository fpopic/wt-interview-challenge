package com.waytation.model

/*
  The RSSI, a negative number, is an indirect measure for the proximity of a tag to a station.
  E.g.: Tag 101 is scanned by two different stations at 13:00:00.
  Station 1 receives a signal with a RSSI of -60, station 2 one with a RSSI of -70.
  It stands to reason that tag 101 was closer to station 1 than to station 2.
*/

// 1465992149744,-91,53,2027
case class Signal(timestamp: Long, rssi: Int, stationId: Int, tagId: Int)

object Signal extends Parseable with Serializable {

  def isParsable(parts: Array[String]): Boolean = {
    try {
      parts(0).toLong
      assert(parts(1).toInt <= 0)
      parts(2).toInt
      parts(3).toInt
    }
    catch {
      case (_: AssertionError) =>
        println(s"Signal's RSSI level not a negative number: $parts")
        return false
      case (_: NumberFormatException) =>
        println(s"Signal not parsable: $parts")
        return false
    }
    true
  }

}