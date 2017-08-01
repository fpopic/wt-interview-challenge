package com.waytation.conference.jobs

import java.io.File

import com.typesafe.config.ConfigFactory
import com.waytation.conference.connection.{CSVFileConnector, MySQLConnector}
import com.waytation.conference.datasource.{StructuredDatasource, UnstructuredDatasource}
import com.waytation.conference.model.Signal
import org.apache.spark.sql.SparkSession

object DatastoreJob {

  def main(args: Array[String]): Unit = {

    //DatastorePreJob.main(null)

    implicit val spark =
      SparkSession.builder
        .master("local[*]")
        .appName("WaytationDatastoreJob")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate

    import spark.implicits._

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file)

    val inConnector = new CSVFileConnector(config.getConfig("csv"))
    val inDS = new UnstructuredDatasource(inConnector)

    val zones = inDS.getZones
    val tags = inDS.getTags
    val stations = inDS.getStations

    val signals = inDS.getSignals
      .join(stations.select('id), 'stationId === 'id).drop('id)
      .join(tags.select('id), 'tagId === 'id).drop('id)
      .distinct
      .as[Signal]

    println(s"Stations:${stations.count()}")
    println(s"Zones:${zones.count()}")
    println(s"Tags:${tags.count()}")
    println(s"Signals:${signals.count()}")

    //    val outConnector = new MySQLConnector(config.getConfig("mysql"))
    //    val outDS = new StructuredDatasource(outConnector)
    val outConnector = new CSVFileConnector(config.getConfig("csv"))
    val outDS = new UnstructuredDatasource(outConnector)

    outDS.writeZones(zones.coalesce(1))
    outDS.writeTags(tags.coalesce(1))
    outDS.writeStations(stations.coalesce(1))
    outDS.writeSignals(signals.coalesce(1))

  }
}