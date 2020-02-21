package com.torres.kafka


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectApiAuto01 {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectApiAuto01").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    //3.kafka参数
    val kafkaConf: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "torres.sun"
    )
    //4.读取kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaConf,Set("torres001"))

    //5.计算wordCount
    kafkaDStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    //6.启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
