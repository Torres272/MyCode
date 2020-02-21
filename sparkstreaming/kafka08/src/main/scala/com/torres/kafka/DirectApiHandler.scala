package com.torres.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectApiHandler {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectApiAuto01").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    //3.kafka参数
    val kafkaConf: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "torres.sun"
    )
    //4.获取上一次启动最后保留的Offset
    val fromOffset: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long](TopicAndPartition("torres001", 0) -> 9)

    //5.读取kafka数据创建DStream
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaConf, fromOffset, (x:MessageAndMetadata[String,String] )=> x.message())

    //6.创建数组存放offset信息
    var offsetRange: Array[OffsetRange] = Array.empty[OffsetRange]

//    //7.获取当前消费数据的offset信息
//    val wordToCountDStream: DStream[(String, Int)] = kafkaDStream.transform(
//      x => {
//        offsetRange = x.asInstanceOf[HasOffsetRanges].offsetRanges
//        x
//      }
//    ).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //7.获取当前消费数据的offset信息
    val wordToCountDStream: DStream[(String, Int)] = kafkaDStream.transform(
      x => {
        offsetRange = x.asInstanceOf[HasOffsetRanges].offsetRanges
        x.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      }
    )


    //8.打印Offset信息
    wordToCountDStream.foreachRDD(x=>{
      for(o <- offsetRange){
        println(s"${o.topic}:${o.fromOffset}:${o.untilOffset}")
      }
      x.foreach(println)
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
