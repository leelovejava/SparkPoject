package com.lihaogn.sparkKafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * spark streaming & kafka -> direct
  */
object KafKaDirectWC {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("usage: KafKaDirectWC <brokers> <topics>")
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // spark straming 对接 kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
