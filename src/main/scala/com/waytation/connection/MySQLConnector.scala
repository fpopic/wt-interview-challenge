package com.waytation.connection

import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class MySQLConnector(val config: Config) extends DataConnector {

  val url: String =
    s"""jdbc:mysql://${config.getString("host")}
       |:${config.getString("port")}
       |;databaseName=${config.getString("databaseName")}
       |;user=${config.getString("user")}
       |;password=${config.getString("password")}""".stripMargin

  private val emptyProperties: Properties = new Properties()

  def readDataFrame(table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.jdbc(url, table, emptyProperties)
  }

  def writeDataFrame(dataFrame: DataFrame, table: String, mode: SaveMode): Unit = {
    dataFrame.write.mode(mode).jdbc(url, table, emptyProperties)
  }
}
