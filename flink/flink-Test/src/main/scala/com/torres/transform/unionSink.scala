package com.torres.transform

import org.apache.flink.streaming.api.scala._


object unionSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dataDS: DataStream[(String, Int)] = env.fromCollection(List(("a", 1), ("b", 2), ("a", 4), ("b", 3)))
        val dataDS1: DataStream[(String, Int)] = env.fromCollection(List(("c", 1), ("c", 2), ("d", 4), ("d", 3)))

        //Todo union数据类型要一样
        dataDS.union(dataDS1).print()


        //dataDS.print()
        env.execute()
    }
}
