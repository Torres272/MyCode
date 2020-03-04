package com.torres.flink

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Flink25_API_WindowFun_Process {
    def main(args: Array[String]): Unit = {
        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(2)

        //读取文件
        val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

        socketDS.map((_,1))
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .process(new MyProcessWindow)
          .print()

        env.execute("app")
    }
}

class MyProcessWindow extends ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Int)],
                          out: Collector[String]
                        ): Unit = {
        out.collect(elements.toList.toString())
    }
}

//// 这个函数类处理数据会更加灵活。
//class MyProcessWindow extends ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
//    override def process(
//                          key: String, // 数据的key
//                          context: Context, // 上下文环境
//                          elements: Iterable[(String, Int)], //窗口中所有相同key的数据
//                          out: Collector[String] // 收集器 用于将数据输出到Sink
//                        ): Unit = {
//        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        out.collect("窗口启动时间:" + sdf.format(new Date(context.window.getStart)))
//        out.collect("窗口结束时间:" + sdf.format(new Date(context.window.getEnd)))
//        out.collect("计算的数据为 ：" + elements.toList)
//        out.collect("***********************************")
//    }
//}