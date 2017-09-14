package com.wt.conference.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class CSVFileConnector(val config: Config) extends DataConnector {

  val readUrl: String = config.getString("in-folder")

  val writeUrl : String = config.getString("out-folder")

  def read(file: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.csv(s"$readUrl/$file.csv")
  }

  def write(ds: Dataset[_], file: String, mode: SaveMode): Unit = {
    ds.write.csv(s"$writeUrl/$file.csv")
  }

}
