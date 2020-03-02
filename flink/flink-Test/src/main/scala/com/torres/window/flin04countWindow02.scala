package com.torres.window

import org.apache.flink.streaming.api.scala._

object flin04countWindow02 {
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
          .countWindow(5,2)
          .reduce((x, y) => {
              println(System.currentTimeMillis()-time1)
              (x._1, x._2 + y._2)
          })
        value.print()

        env.execute("app")
    }
}
