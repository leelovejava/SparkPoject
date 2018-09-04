package com.lihaogn.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 处理文件体统（local/HDFS）中的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("file:///Users/Mac/testdata/spark-streaming/")

    val results = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    results.print()

    ssc.start()

    ssc.awaitTermination()


  }
}
