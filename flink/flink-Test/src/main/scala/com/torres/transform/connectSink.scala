package com.torres.transform

import org.apache.flink.streaming.api.scala._


object connectSink {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //Todo connect的用法，最后需要变换成相同的结构才能打印
        val dataDS: DataStream[(String,Int)] = env.fromCollection(List(("a",1),("b",2),("a",4),("b",3)))
        val dataDS1: DataStream[Int] = env.fromCollection(List(1,2,3,4))

        val conn: ConnectedStreams[(String, Int), Int] = dataDS.connect(dataDS1)
        conn.map(x=>x,y=>("y",y)).print()


        //dataDS.print()
        env.execute()
    }
}
