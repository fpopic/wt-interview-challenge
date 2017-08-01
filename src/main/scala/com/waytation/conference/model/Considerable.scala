package com.waytation.conference.model

import org.apache.spark.sql.Row

trait Considerable {

  def isConsiderable(row: Row): Boolean

}