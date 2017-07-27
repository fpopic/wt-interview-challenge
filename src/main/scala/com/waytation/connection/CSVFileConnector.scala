package com.waytation.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class CSVFileConnector(val config: Config) extends DatasetConnector {

  val url: String = config.getString("folder")

  def read(file: String)(implicit spark: SparkSession): Dataset[String] = {
    spark.read.textFile(s"$url/$file.csv")
  }

  def write(ds: Dataset[_], file: String, mode: SaveMode): Unit = {
    ds.write.csv(s"$url/$file.csv")
  }

}
