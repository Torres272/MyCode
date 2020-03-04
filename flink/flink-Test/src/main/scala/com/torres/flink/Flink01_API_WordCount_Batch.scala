package com.torres.flink

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object Flink01_API_WordCount_Batch {
  def main(args: Array[String]): Unit = {
    //创建运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //创建input文件夹，读取文件
    val fileDS: DataSet[String] = env.readTextFile("input/word.txt")

    //对数据进行处理
    val wordDS: DataSet[String] = fileDS.flatMap(_.split(" "))
    val wordToOneDS: DataSet[(String, Int)] = wordDS.map((_,1))
    val resultDS: AggregateDataSet[(String, Int)] = wordToOneDS.groupBy(0).sum(1)

    //打印
    resultDS.print()
  }
}
