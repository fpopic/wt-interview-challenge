package com.waytation.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CSVConnector(val config: Config) extends DataConnector {

  val url: String = config.getString("folder")

  def readDataFrame(file: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.text(s"$url/$file")
  }

  def writeDataFrame(dataFrame: DataFrame, file: String, mode: SaveMode): Unit = {
    dataFrame.write.csv(s"$url/$file")
  }

}
