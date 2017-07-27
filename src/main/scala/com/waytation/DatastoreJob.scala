package com.waytation

import java.io.File
import java.sql.{DriverManager, Timestamp}

import com.typesafe.config.ConfigFactory
import com.waytation.connection.{CSVFileConnector, MySQLConnector}
import com.waytation.datasource.{StructuredDatasourceManager, UnstructuredDatasourceReader}
import com.waytation.model.Signal
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

object DatastoreJob {

  def main(args: Array[String]): Unit = {

    DatastorePreJob.main(null)

    implicit val spark =
      SparkSession.builder
        .master("local[*]")
        .appName("WaytationDatastoreJob")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate

    import spark.implicits._

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file)

    val csvConnector = new CSVFileConnector(config.getConfig("csv"))
    val fileDS = new UnstructuredDatasourceReader(csvConnector)

    val zones = fileDS.getZones
    val tags = fileDS.getTags
    val stations = fileDS.getStations

    val signals = fileDS.getSignals
      .join(stations.select('id), 'stationId === 'id).drop('id)
      .join(tags.select('id), 'tagId === 'id).drop('id)
      .distinct
      .as[Signal]

    val mysqlConnector = new MySQLConnector(config.getConfig("mysql"))
    val mysqlDS = new StructuredDatasourceManager(mysqlConnector)

    mysqlDS.writeZones(zones)
    mysqlDS.writeTags(tags)
    mysqlDS.writeStations(stations)
    mysqlDS.writeSignals(signals)

  }
}