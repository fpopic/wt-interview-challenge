package com.waytation.datasource

import com.waytation.model.{Signal, Station, Tag, Zone}
import org.apache.spark.sql.Dataset

object ConferenceDatasource {

  def getSignals: Dataset[Signal] = null

  def getTags: Dataset[Tag] = null

  def getStations: Dataset[Station] = null

  def getZones: Dataset[Zone] = null

  def saveSignals(signals: Dataset[Signal]): Unit = {}

}
