package com.torres.transform

import org.apache.flink.streaming.api.scala._


object aggregationSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataDS: DataStream[(String,Int)] = env.fromCollection(List(("a",1),("b",2),("a",4),("b",3)))

        //TODO 流的形式，每条求最大值，每条求和
        dataDS
          .keyBy(_._1)
          .sum(1)
          //.max(1)
          .print()

        //dataDS.print()
        env.execute()
    }
}
