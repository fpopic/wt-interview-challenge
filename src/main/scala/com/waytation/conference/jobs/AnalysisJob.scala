package com.waytation.conference.jobs

import java.io.File
import java.sql.Timestamp

import com.typesafe.config.ConfigFactory
import com.waytation.conference.connection.CSVFileConnector
import com.waytation.conference.datasource.UnstructuredDatasource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AnalysisJob {

  val rssiPresenceTreshold: Int = -85

  def computeMostPopularZoneForEachDay(joinedSignals: DataFrame): Dataset[(Int, String)] = {

    import joinedSignals.sparkSession.implicits._

    // OK with dummy rssi criteria
    val joined = joinedSignals
      .withColumn("day", dayofyear('timestamp))
      .select('day, 'name as 'room, 'rssi)

    // day of conference = dayofyear - min(dayofyear) + 1
    val minDay = joined.select('day).agg(min('day)).first().getInt(0)

    joined
      .where('rssi >= rssiPresenceTreshold)
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

  def computeDistinctVisitors(joinedSignals: DataFrame): Long = {
    import joinedSignals.sparkSession.implicits._
    joinedSignals
      .select('tagId)
      .distinct()
      .count()
  }

  def computeDistinctVisitors30min(joinedSignals: DataFrame): Long = {
    import joinedSignals.sparkSession.implicits._

    // in miliseconds
    val oneMin = 60 * 1000L
    val thirtyMin = 30 * oneMin

    joinedSignals
      .select('timestamp, 'tagId as 'user, 'name as 'room, 'rssi)
      .where('rssi >= rssiPresenceTreshold)
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
          case _ => {
            println(user, room)
            0L
          }
        }
      (user, timeInRoom)
    }
      .filter(_._2 >= thirtyMin) // by time
      .map(_._1) // to user
      .distinct()
      .count()
  }

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("WaytationAnalsisJob")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate

    import spark.implicits._

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file).getConfig("csv")
    val connector = new CSVFileConnector(config)
    val ds = new UnstructuredDatasource(connector)

    val stations = ds.getStations
    val zones = ds.getZones
    val tags = ds.getTags
    val signals = ds.getSignals

    val joinedSignals = signals
      .join(stations, 'stationId === 'id)
      .drop("id")
      .join(zones, 'zoneId === 'id)
      .drop("id")
      .join(tags, 'tagId === 'id)
      .drop("id")
      .cache()

    //    val firstAnalysis = computeMostPopularZoneForEachDay(joinedSignals)
    //    firstAnalysis.show(false)
    //    firstAnalysis.coalesce(1).rdd.saveAsTextFile("result/first.txt")

    //    val distinctVisitors = computeDistinctVisitors(joinedSignals)
    //    val secondAnalysis = Seq(distinctVisitors).toDF("visitors")
    //    secondAnalysis.show(false)
    //    secondAnalysis.coalesce(1).rdd.saveAsTextFile("result/second.txt")

    val distinctVisitors30 = computeDistinctVisitors30min(joinedSignals)
    val thirdAnalysis = Seq(distinctVisitors30).toDF("visitors30min")
    thirdAnalysis.show(false)
  //  thirdAnalysis.coalesce(1).rdd.saveAsTextFile("result/third.txt")
  }
}