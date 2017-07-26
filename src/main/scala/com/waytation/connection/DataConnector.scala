package com.waytation.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


trait DataConnector {

  val config: Config

  val url: String

  def readDataFrame(name: String)(implicit spark: SparkSession): DataFrame

  def writeDataFrame(dataFrame : DataFrame, table : String, mode : SaveMode) : Unit

}