package com.torres.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink15_API_Sink_Kafka {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataDS: DataStream[String] = env.readTextFile("input/word.txt")

        // 向Kafka中输出内容
        dataDS.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "sensor", new SimpleStringSchema()))

        dataDS.print()
        env.execute()
    }
}
