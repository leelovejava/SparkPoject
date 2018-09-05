package com.lihaogn.sparkFlume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming & flume => push
  */
object FlumePushWC {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("usage: flumepushwc <hostname> <por>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf() //.setMaster("local[2]").setAppName("FlumePushWC")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 使用sparkstreaming整合flume
    //    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 41414)
    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()


  }

}
