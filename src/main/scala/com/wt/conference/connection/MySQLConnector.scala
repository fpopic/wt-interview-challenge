package com.wt.conference.connection

import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

class MySQLConnector(val config: Config) extends DataConnector {

  Class.forName(config.getString("driver"))

  val readUrl: String =
    s"jdbc:mysql://" +
      s"${config.getString("host")}" +
      s":${config.getString("port")}" +
      s"/${config.getString("database")}"

  val writeUrl: String = readUrl

  private val props = {
    val p = new Properties()
    p.setProperty("driver", config.getString("driver"))
    p.setProperty("user", config.getString("username"))
    p.setProperty("password", config.getString("password"))
    p.setProperty("useSSL", "false")
    p
  }

  def read(table: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.jdbc(readUrl, table, props)
  }

  def write(ds: Dataset[_], table: String, mode: SaveMode): Unit = {
    ds.write.mode(mode).jdbc(writeUrl, table, props)
  }

}