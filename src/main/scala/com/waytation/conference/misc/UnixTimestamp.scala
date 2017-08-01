package com.waytation.conference.misc

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object UnixTimestamp {

  /**
    * MySQL min timestamp value
    */
  private lazy val min: Timestamp = Timestamp.valueOf("1970-01-01 01:00:00.0")

  /**
    * MySQL max timestamp value
    */
  private lazy val max: Timestamp = Timestamp.valueOf("2038-01-19 03:14:07.0")

  /**
    *
    * @param timestamp timestamp to validate
    *
    * @return boolean flag if ''t'' is a valid MySQL timestamp instance
    */
  def isValidTimestamp(timestamp: Timestamp): Boolean = {
    if (timestamp.after(min) && timestamp.before(max)) true
    else false
  }

  /**
    * Convert time string to a Unix timestamp (in miliseconds).
    * Proxy for conversion: [[TimestampType]] => [[LongType]]
    */
  val timestamp_to_milis: UserDefinedFunction = udf {(_: Timestamp).getTime}

}
