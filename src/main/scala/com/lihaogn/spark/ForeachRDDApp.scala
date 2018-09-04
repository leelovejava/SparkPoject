package com.lihaogn.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 完成词频统计，并将结果写入MySQL
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val results = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    results.print()

    results.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word,wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })

    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取MySQL连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkSql_project", "root", "rootroot")

  }
}
