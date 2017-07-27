package com.waytation.misc

import java.sql.Timestamp

object UnixTimestamp {

  private lazy val min: Timestamp = Timestamp.valueOf("1970-01-01 01:00:00.0")
  private lazy val max: Timestamp = Timestamp.valueOf("2038-01-19 03:14:07.0")

  def isValid(t: Timestamp): Boolean = {
    if (t.after(min) && t.before(max)) true
    else false
  }

}
