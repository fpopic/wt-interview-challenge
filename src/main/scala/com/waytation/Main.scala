package com.waytation

import com.typesafe.config.ConfigFactory
import com.waytation.connection.{CSVConnector, DataConnector}
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

object Main {

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder
      .appName("WaytationChallengeSparkJob")
      .master("local[*]")
      .getOrCreate

    val config = ConfigFactory
      .parseResources("database.json")
      .getConfig("csv")

    val connector: DataConnector = new CSVConnector(config)



    val a = connector.readDataFrame("tags.csv")

    println(a.count())


  }
}