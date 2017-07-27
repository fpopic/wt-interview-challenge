package com.waytation.datasource

import java.sql.Timestamp
import com.waytation.connection.DatasetConnector
import com.waytation.model.{Signal, Station, Tag, Zone}
import org.apache.spark.sql.{Dataset, SparkSession}

//noinspection ConvertibleToMethodValue
class UnstructuredDatasourceReader(val dc: DatasetConnector)
  (implicit spark: SparkSession) extends DatasourceReader {

  import spark.implicits._

  def getSignals: Dataset[Signal] = {
    dc.read("signals")
      .map(_.split(","))
      .filter(Signal.isParsable(_))
      .filter(Signal.isConsiderable(_))
      .map(parts => Signal(
        timestamp = new Timestamp(parts(0).toLong),
        rssi = parts(1).toInt,
        stationId = parts(2).toInt,
        tagId = parts(3).toInt)
      )
  }

  def getTags: Dataset[Tag] = {
    dc.read("tags")
      .map(_.split(","))
      .filter(Tag.isParsable(_))
      .filter(Tag.isConsiderable(_))
      .map(parts => Tag(
        id = parts(0).toInt,
        activeFrom = new Timestamp(parts(1).toLong),
        activeTo = new Timestamp(parts(2).toLong))
      )
  }

  def getStations: Dataset[Station] = {
    dc.read("stations")
      .map(_.split(","))
      .filter(Station.isParsable(_))
      .map(parts => Station(
        id = parts(0).toInt,
        zoneId = parts(1).toInt)
      )
  }

  def getZones: Dataset[Zone] = {
    dc.read("zones")
      .map(_.split(","))
      .filter(Zone.isParsable(_))
      .map(parts => Zone(
        id = parts(0).toInt,
        name = parts(1))
      )
  }

}