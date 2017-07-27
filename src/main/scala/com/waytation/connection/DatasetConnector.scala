package com.waytation.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


trait DatasetConnector {

  val config: Config

  val url: String

  def read(name: String)(implicit spark: SparkSession): Dataset[String]

  def write(ds: Dataset[_], table: String, mode: SaveMode): Unit

}