package com.waytation.conference.jobs

import java.io.File
import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import com.waytation.conference.connection.CSVFileConnector
import com.waytation.conference.datasource.UnstructuredDatasource
import com.waytation.conference.model.Signal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AnalysisJob {

  // average rssi = -82

  private val rssiPresenceThreshold = -85

  def computeMostPopularZoneForEachDay(richsignals: DataFrame): Dataset[(Int, String)] = {
    import richsignals.sparkSession.implicits._

    val joined = richsignals
      .withColumn("day", dayofyear('timestamp))
      .select(
        'day,
        'name as 'room,
        'rssi
      )

    // day of conference = dayofyear - min(dayofyear) + 1
    val minDay = joined.select('day).agg(min('day)).first().getInt(0)

    joined
      .where('rssi >= rssiPresenceThreshold)
      .groupBy('day, 'room)
      .agg(count('day), ('day - minDay + 1) as "orderedDay")
      .withColumn("day", 'orderedDay).drop('orderedDay)
      .as[(Int, String, Long)] // (day, room, count)
      // by day
      .groupByKey { case (day, _, _) => day }
      // sort by count, map to (day, room)
      .mapGroups((day, group) => (day, group.toArray.sortWith(_._3 >= _._3).head._2))
      .toDF("day", "room")
      .as[(Int, String)]
      .orderBy('day)
  }

  def computeDistinctVisitors(signals: Dataset[Signal]): Long = {
    import signals.sparkSession.implicits._
    signals
      .select('tagId)
      .distinct()
      .count()
  }

  def computeDistinctVisitors30min(richSignals: DataFrame): Long = {
    import richSignals.sparkSession.implicits._

    // in miliseconds
    val oneMin = 60 * 1000L
    val thirtyMin = 30 * oneMin

    richSignals
      .select(
        'timestamp,
        'tagId as 'user,
        'name as 'room,
        'rssi
      )
      .where('rssi >= rssiPresenceThreshold)
      .drop('rssi)
      .as[(Timestamp, Int, String)]

      .groupByKey(t => (t._3, t._2)) // by user, room
      .mapGroups { case ((user, room), records) =>
      val timeInRoom = records.toStream
        .map(_._1.getTime) // to miliseconds
        .sorted
        .sliding(size = 2)
        .foldLeft(0L) {
          case (total, Stream(t1, t2)) =>
            val elapsed = t2 - t1
            if (elapsed <= oneMin) total + elapsed
            else total
          case _ =>
            0L
        }
      (user, timeInRoom)
    }
      .filter(_._2 >= thirtyMin) // by time
      .map(_._1) // to user
      .distinct()
      .count()
  }

  def computeAveragePresentTagsPerHour(signals: Dataset[Signal]): Dataset[(Int, Int)] = {
    import signals.sparkSession.implicits._

    val (firstDay, lastDay) = signals
      .select(dayofyear('timestamp) as 'day)
      .agg(min('day), max('day))
      .as[(Int, Int)]
      .first()

    val numOfDays = lastDay - firstDay + 1

    signals
      .select(
        hour('timestamp) as "hour",
        'tagId,
        'rssi
      )
      .where('rssi >= rssiPresenceThreshold)
      .drop('rssi)
      .groupBy('hour)
      .agg(countDistinct('tagId) / numOfDays cast IntegerType as "average present tags")
      .orderBy('hour)
      .as[(Int, Int)]
  }

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("WaytationAnalsisJob")
      .getOrCreate

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file).getConfig("csv")
    val connector = new CSVFileConnector(config)
    val ds = new UnstructuredDatasource(connector)

    val stations = ds.getStations
    val zones = ds.getZones
    val tags = ds.getTags
    val signals = ds.getSignals.cache()

    import spark.implicits._

    val richSignals = signals
      .join(stations, 'stationId === 'id).drop("id")
      .join(zones, 'zoneId === 'id).drop("id")
      .join(tags, 'tagId === 'id).drop("id")
      .cache()

    val firstAnalysis = computeMostPopularZoneForEachDay(richSignals)
    firstAnalysis.show(false)

    val distinctVisitors = computeDistinctVisitors(signals)
    val secondAnalysis = Seq(distinctVisitors).toDF("visitors")
    secondAnalysis.show(false)

    val distinctVisitors30 = computeDistinctVisitors30min(richSignals)
    val thirdAnalysis = Seq(distinctVisitors30).toDF("visitors30min")
    thirdAnalysis.show(false)

    val averageTagsPerHour = computeAveragePresentTagsPerHour(signals)
    averageTagsPerHour.show(false)
  }

}