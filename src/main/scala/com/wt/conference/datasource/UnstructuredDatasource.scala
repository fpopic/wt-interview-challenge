package com.wt.conference.datasource

import java.sql.Timestamp

import com.wt.conference.connection.DataConnector
import com.wt.conference.model.{Signal, Station, Tag, Zone}
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{Dataset, SparkSession}

//noinspection ConvertibleToMethodValue
class UnstructuredDatasource(val dc: DataConnector)
  (implicit spark: SparkSession) extends DatasourceReader with DatasourceWriter {

  import spark.implicits._

  def getSignals: Dataset[Signal] = {
    dc.read("signals")
      .filter(Signal.isParsable(_))
      .filter(Signal.isConsiderable(_))
      .map(parts => Signal(
        timestamp = new Timestamp(parts.getString(0).toLong),
        rssi = parts.getString(1).toInt,
        stationId = parts.getString(2).toInt,
        tagId = parts.getString(3).toInt)
      )
  }

  def getTags: Dataset[Tag] = {
    dc.read("tags")
      .filter(Tag.isParsable(_))
      .filter(Tag.isConsiderable(_))
      .map(parts => Tag(
        id = parts.getString(0).toInt,
        activeFrom = new Timestamp(parts.getString(1).toLong),
        activeTo = new Timestamp(parts.getString(2).toLong))
      )
  }

  def getStations: Dataset[Station] = {
    dc.read("stations")
      .filter(Station.isParsable(_))
      .map(parts => Station(
        id = parts.getString(0).toInt,
        zoneId = parts.getString(1).toInt)
      )
  }

  def getZones: Dataset[Zone] = {
    dc.read("zones")
      .filter(Zone.isParsable(_))
      .map(parts => Zone(
        id = parts.getString(0).toInt,
        name = parts.getString(1))
      )
  }

  def writeSignals(signals: Dataset[Signal]): Unit = {
    val signalsWithLongTimestamps = signals
      .withColumn("timestamp", timestamp_to_milis('timestamp))

    dc.write(signalsWithLongTimestamps, "signals", Overwrite)
  }

  def writeTags(tags: Dataset[Tag]): Unit = {
    val tagsWithLongTimestamps = tags
      .withColumn("activeFrom", timestamp_to_milis('activeFrom))
      .withColumn("activeTo", timestamp_to_milis('activeTo))

    dc.write(tagsWithLongTimestamps, "tags", Overwrite)
  }

  def writeStations(stations: Dataset[Station]): Unit = {
    dc.write(stations, "stations", Overwrite)
  }

  def writeZones(zones: Dataset[Zone]): Unit = {
    dc.write(zones, "zones", Overwrite)
  }

}