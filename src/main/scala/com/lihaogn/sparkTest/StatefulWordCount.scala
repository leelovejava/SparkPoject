package com.lihaogn.sparkTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用spark streaming 完成所有状态统计
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    // 若使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint("/Users/Mac/testdata/spark-streaming-checkpoint")

    val lines=ssc.socketTextStream("localhost",6789)

    val results=lines.flatMap(_.split(" ")).map((_,1))

    val state=results.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 当前的数据去更新老的数据
    * @param currentValues
    * @param preValues
    * @return
    */
  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int]={
    val current=currentValues.sum
    val pre=preValues.getOrElse(0)

    Some(current+pre)
  }

}
