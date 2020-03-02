package com.torres.transform

import org.apache.flink.streaming.api.scala._


object splitSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataDS: DataStream[String] = env.readTextFile("input/word.txt")

        dataDS
          .flatMap(_.split(" "))
          .map((_, 1)).keyBy(0)
          .split(x => {
              if (x._1.size >= 3) {
                  List("big","long")
              } else {
                  Seq("small")
              }
          })
          .select("long")
          .print()

        //dataDS.print()
        env.execute()
    }
}
