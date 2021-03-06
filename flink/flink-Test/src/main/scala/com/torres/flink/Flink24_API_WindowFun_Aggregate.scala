package com.torres.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._

object Flink24_API_WindowFun_Aggregate {
    def main(args: Array[String]): Unit = {
        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(2)

        //读取文件
        val fileDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)
        val wordDS: DataStream[String] = fileDS.flatMap(_.split(" "))

        val time1: Long = System.currentTimeMillis()

        //wordCount
        wordDS.map((_, 1))
          .keyBy(0)
          .countWindow(5, 2)
          .aggregate(new MyAggregateFunction)
          .print()


        env.execute("app")
    }
}

class MyAggregateFunction extends AggregateFunction[(String, Int), Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(value: (String, Int), accumulator: Int): Int = {
        value._2 + accumulator
    }

    override def getResult(accumulator: Int): Int = accumulator

    override def merge(a: Int, b: Int): Int = a + b
}