package com.torres.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectApiAuto02 {

  var getSSC: () => StreamingContext = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectApiAuto02").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //ssc.checkpoint("./ck")

    //3.kafka参数
    val kafkaConf: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "torres.sun"
    )
    //4.读取kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("torres001"))

    //5.计算wordCount
    kafkaDStream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    () => ssc
  }


  def main(args: Array[String]): Unit = {


    //一种新的获取ssc的方式
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", getSSC)

    ssc.start()
    ssc.awaitTermination()
  }
}
