package com.torres.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink02_API_WordCount_Bounded {
  def main(args: Array[String]): Unit = {
    //构建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //读取文件
    val fileDS: DataStream[String] = env.readTextFile("input/word.txt")
    val wordDS: DataStream[String] = fileDS.flatMap(_.split(" "))

    //TODO wordCount
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_,1))
    wordToOneDS.keyBy(0).sum(1).print()

    env.execute("app")
  }
}
