package com.waytation.datasource

import com.waytation.model.{Signal, Station, Tag, Zone}
import org.apache.spark.sql.Dataset

trait DatasourceWriter {

  def writeSignals(signals: Dataset[Signal]): Unit

  def writeTags(tags: Dataset[Tag]): Unit

  def writeStations(stations: Dataset[Station]): Unit

  def writeZones(zones: Dataset[Zone]): Unit

}