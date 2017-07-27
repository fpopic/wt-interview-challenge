package com.waytation

import java.io.File
import java.sql.DriverManager

import com.typesafe.config.ConfigFactory

object DatastorePreJob {

  def main(args: Array[String]): Unit = {

    val file = new File("datastore.json")
    val config = ConfigFactory.parseFile(file).getConfig("mysql")

    val url: String =
      s"jdbc:mysql://" +
        s"${config.getString("host")}" +
        s":${config.getString("port")}" +
        s"/${config.getString("database")}" +
        s"?useSSL=false"

    val conn = DriverManager.getConnection(
      url,
      config.getString("username"),
      config.getString("password")
    )

    val query = conn.createStatement()
    query.addBatch("DELETE FROM tags;")
    query.addBatch("DELETE FROM stations;")
    query.addBatch("DELETE FROM zones;")
    query.addBatch("DELETE FROM signals;")

    val affected = query.executeBatch().toSeq

    println(s"Rows affected Count = $affected")

    conn.close()
  }

}