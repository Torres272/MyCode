package com.torres.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverAPI {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectApiAuto01").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
//    //3.kafka参数
//    val kafkaConf: Map[String, String] = Map[String, String](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
//      ConsumerConfig.GROUP_ID_CONFIG -> "torres.sun"
//    )

    //3.读取Kafka数据创建DStream
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "hadoop102:2181,hadoop103:2181,hadoop104:2181", "torres001", Map[String, Int]("torres001" -> 1))
    kafkaDStream.flatMap(_._2.split(" ")).map((_,1)).reduceByKey(_+_).print()

    //5.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
