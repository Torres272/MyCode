package com.torres.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


object Flink30_API_WaterMark_EventTime {
    def main(args: Array[String]): Unit = {

        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置并行度
        env.setParallelism(1)
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //读取文件
        val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)


        socketDS.print()
        env.execute("app")
    }
}

