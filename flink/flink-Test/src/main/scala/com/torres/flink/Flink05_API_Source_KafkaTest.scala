package com.torres.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object Flink05_API_Source_KafkaTest {
  def main(args: Array[String]): Unit = {
    //定义env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //定义kafka参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //读取kafka数据
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensorWater",new SimpleStringSchema( ),properties))

    //打印数据
    kafkaDS.print("kafka=>")
    env.execute("KafkaSourceTest")



  }
}
