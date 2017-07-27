package com.waytation.connection

import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class MySQLConnector(val config: Config) extends DatasetConnector {

  Class.forName(config.getString("driver"))

  val url: String =
    s"jdbc:mysql://" +
      s"${config.getString("host")}" +
      s":${config.getString("port")}" +
      s"/${config.getString("database")}"

  private val props = {
    val p = new Properties()
    p.setProperty("driver", config.getString("driver"))
    p.setProperty("user", config.getString("username"))
    p.setProperty("password", config.getString("password"))
    p.setProperty("useSSL", "false")
    p
  }

  def read(table: String)(implicit spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    spark.read.jdbc(url, table, props).as[String]
  }

  def write(ds: Dataset[_], table: String, mode: SaveMode): Unit = {
    ds.write.mode(mode).jdbc(url, table, props)
  }

}