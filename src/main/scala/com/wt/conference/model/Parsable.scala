package com.wt.conference.model

import org.apache.spark.sql.Row

trait Parsable {

  def isParsable(row: Row): Boolean

}