package com.torres.flink

import com.torres.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Flink36_API_WaterMark_Punctuated {
    def main(args: Array[String]): Unit = {

        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置并行度
        env.setParallelism(1)
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //设定watermark的生命周期
        env.getConfig.setAutoWatermarkInterval(3000)

        //读取文件
        val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)


        //封装样例类
        val sensorDS: DataStream[WaterSensor] = socketDS.map(x => {
            val data: Array[String] = x.split(",")
            WaterSensor(data(0), data(1).toLong, data(2).toDouble)
        })


        //抽取时间戳和设定waterMark
        val markDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            //            new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
            //                override def extractTimestamp(element: WaterSensor): Long = {
            //                    element.ts * 1000L
            //                }
            //            }

          //  /ˈpʌŋktʃueɪt/
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                private var currentTS = 0L

                override def checkAndGetNextWatermark
                (lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    println("watermark" + new Watermark(currentTS).getTimestamp)
                    new Watermark(currentTS)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    println("extractTimeStamp")
                    currentTS = element.ts * 1000L
                    element.ts * 1000L
                }
            }
        )

        //keyBy
        val keyByDS: KeyedStream[WaterSensor, String] = markDS.keyBy(_.id)

        //开窗
        val windowDS: WindowedStream[WaterSensor, String, TimeWindow] = keyByDS.timeWindow(Time.seconds(5))

        //apply方法
        val applyDS: DataStream[String] = windowDS.apply((key: String, window: TimeWindow, datas: Iterable[WaterSensor], out: Collector[String]) => {
            //val sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
            println(window.maxTimestamp())
            out.collect(s"${window.getStart}-${window.getEnd}--数据：${datas.toList.toString()}")
        })

        //输出
        markDS.print("mark>>")
        //keyByDS.print("key>>")
        applyDS.print("apply>>")


        env.execute("app")
    }
}

