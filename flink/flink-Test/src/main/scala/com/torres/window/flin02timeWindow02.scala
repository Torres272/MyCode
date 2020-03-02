package com.torres.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object flin02timeWindow02 {
    def main(args: Array[String]): Unit = {
        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(2)

        //读取文件
        val fileDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)
        val wordDS: DataStream[String] = fileDS.flatMap(_.split(" "))

        val time1: Long = System.currentTimeMillis()

        //wordCount
        val value: DataStream[(String, Int)] = wordDS.map((_, 1))
          .keyBy(0)
          .timeWindow(Time.seconds(5),Time.seconds(2))
          .reduce((x, y) => {
              println(System.currentTimeMillis()-time1)
              (x._1, x._2 + y._2)
          })
        value.print()

        env.execute("app")
    }
}
