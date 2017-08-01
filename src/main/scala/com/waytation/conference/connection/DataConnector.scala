package com.waytation.conference.connection

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}


trait DataConnector {

  val config: Config

  val readUrl: String

  val writeUrl: String

  /**
    *
    * @param name  file or table name
    * @param spark [[SparkSession]] instance
    *
    * @return [[DataFrame]] representing rows of file or table
    */
  def read(name: String)(implicit spark: SparkSession): DataFrame

  /**
    *
    * @param dataset dataset to write to file or table
    * @param uri     where to store the dataset
    * @param mode    saving mode
    */
  def write(dataset: Dataset[_], uri: String, mode: SaveMode): Unit

}