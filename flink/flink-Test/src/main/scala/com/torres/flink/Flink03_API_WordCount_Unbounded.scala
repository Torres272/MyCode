package com.torres.flink

import org.apache.flink.streaming.api.scala._

object Flink03_API_WordCount_Unbounded {
  def main(args: Array[String]): Unit = {
    //构建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    //读取文件
    val fileDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
    val wordDS: DataStream[String] = fileDS.flatMap(_.split(" "))

    //wordCount
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_,1))
    wordToOneDS.keyBy(0).sum(1).print("wc")

    env.execute("app")
  }
}
