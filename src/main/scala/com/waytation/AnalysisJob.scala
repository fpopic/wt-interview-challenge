package com.waytation

import java.io.File

import com.typesafe.config.ConfigFactory
import com.waytation.connection.{CSVFileConnector, MySQLConnector}
import com.waytation.datasource.{StructuredDatasourceManager, UnstructuredDatasourceReader}
import org.apache.spark.sql.SparkSession

/*
// 1465992149744,-91,53,2027
case class Signal(timestamp: Long, rssi: Int, stationId: Int, tagId: Int)

// 1888,1465658885000,1465906450000
case class Tag(id: Int, activeForm: Long, aciveTo: Long)

// 17,Room 1
case class Station(id: Int, zoneId: Int)

// 53,17
case class Zone(id: Int, name: String)
*/

object AnalysisJob {

  def main(args: Array[String]): Unit = {

    implicit val spark =
      SparkSession.builder
        .appName("WaytationAnalsisJob")
        .master("local[*]")
        .getOrCreate

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file)

    println(config.entrySet())

  }
}