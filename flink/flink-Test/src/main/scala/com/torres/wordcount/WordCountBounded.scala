package com.torres.wordcount

import org.apache.flink.streaming.api.scala._

object WordCountBounded {
  def main(args: Array[String]): Unit = {
    //构建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //读取文件
    val fileDS: DataStream[String] = env.readTextFile("input/word.txt")
    val wordDS: DataStream[String] = fileDS.flatMap(_.split(" "))

    //wordCount
    val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_,1))
    wordToOneDS.keyBy(0).sum(1).print()

    env.execute("app")
  }
}
