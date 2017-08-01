package com.waytation.conference.jobs

import java.io.File

import com.typesafe.config.ConfigFactory
import com.waytation.conference.connection.CSVFileConnector
import com.waytation.conference.datasource.UnstructuredDatasource
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AnalysisJob {

  def computeMostPopularZoneForEachDay(joinedSignals: DataFrame): Dataset[(Int, String)] = {
    import joinedSignals.sparkSession.implicits._

    val joined = joinedSignals
      .withColumn("day", dayofyear('timestamp))
      .select('day, 'name as 'room)

    // day of conference = dayofyear - min(dayofyear) + 1
    val minDay = joined.select('day).agg(min('day)).first().getInt(0)

    joined
      .groupBy('day, 'room)
      .agg(count('day), ('day - minDay + 1) as "orderedDay")
      .withColumn("day", 'orderedDay).drop('orderedDay)
      .as[(Int, String, Long)] // (day, room, count)

      .groupByKey { case (day, _, _) => day } // by room
      .mapGroups((day, group) => (day, group.toArray.sortWith(_._3 >= _._3).head._2)) // by count
      .toDF("day", "room")
      .as[(Int, String)]
      .orderBy('day)
  }


  def computeDistinctVisitors(joinedSignals: DataFrame): Long = {
    import joinedSignals.sparkSession.implicits._
    joinedSignals.select('tagId).distinct().count()
  }

  def computeDistinctVisitors30min(joinedSignals: DataFrame): Long = {
    import joinedSignals.sparkSession.implicits._

    joinedSignals
    .

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
    //    val secondAnalysis = spark.createDataset(Seq(distinctVisitors)).toDF("visitors")
    //    secondAnalysis.show(false)
    //    secondAnalysis.coalesce(1).rdd.saveAsTextFile("result/second.txt")


  }
}