package com.wt.conference.datasource

import com.wt.conference.connection.DataConnector
import com.wt.conference.model.{Signal, Station, Tag, Zone}
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{Dataset, SparkSession}

class StructuredDatasource(val dc: DataConnector)
  (implicit val spark: SparkSession) extends DatasourceReader with DatasourceWriter {

  import spark.implicits._

  def getSignals: Dataset[Signal] = {
    dc.read("signals")
      .select(
        'timestamp,
        'rssi,
        'stations_id as 'stationId,
        'distributed_tags_id as 'tagId
      )
      .as[Signal]
  }

  def getTags: Dataset[Tag] = {
    dc.read("tags")
      .select(
        'id,
        'active_from as 'activeFrom,
        'active_to as 'activeTo
      )
      .as[Tag]
  }

  def getStations: Dataset[Station] = {
    dc.read("stations")
      .select(
        'id,
        'zones_id as 'zoneId
      )
      .as[Station]
  }

  def getZones: Dataset[Zone] = {
    dc.read("zones")
      .select(
        'id,
        'name
      )
      .as[Zone]
  }

  def writeSignals(signals: Dataset[Signal]): Unit = {
    val renamedSignals = signals.select(
      'timestamp as 'timestamp,
      'rssi,
      'stationId as 'stations_id,
      'tagId as 'distributed_tags_id
    )
    dc.write(renamedSignals, "signals", Append)
  }

  def writeTags(tags: Dataset[Tag]): Unit = {
    val renamedTags = tags.select(
      'id,
      'activeFrom as 'active_from,
      'activeTo as 'active_to
    )
    dc.write(renamedTags, "tags", Append)
  }

  def writeStations(stations: Dataset[Station]): Unit = {
    val renamedStations = stations.select(
      'id,
      'zoneId as 'zones_id
    )
    dc.write(renamedStations, "stations", Append)
  }

  def writeZones(zones: Dataset[Zone]): Unit = {
    dc.write(zones, "zones", Append)
  }

}