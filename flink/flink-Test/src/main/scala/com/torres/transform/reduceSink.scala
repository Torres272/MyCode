package com.torres.transform


import org.apache.flink.streaming.api.scala._


object reduceSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataDS: DataStream[String] = env.readTextFile("input/word.txt")

        dataDS
          .flatMap(_.split(" "))
          .map((_, 1)).keyBy(0)
          .reduce((x, y) => {
              (x._1, x._2 + y._2)
          })
          .print()

        //dataDS.print()
        env.execute()
    }
}
