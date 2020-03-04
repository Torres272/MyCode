package com.torres.flink

import com.torres.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


object Flink39_API_ProcessFun_KeyedProcess {
    def main(args: Array[String]): Unit = {

        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        //设置并行度
        env.setParallelism(1)
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //设定watermark的生命周期
        //env.getConfig.setAutoWatermarkInterval(3000)

        //读取文件
        val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)


        //封装样例类
        val sensorDS: DataStream[WaterSensor] = socketDS.map(x => {
            val data: Array[String] = x.split(",")
            WaterSensor(data(0), data(1).toLong, data(2).toDouble)
        })


        //抽取时间戳和设定waterMark
        val markDS: DataStream[WaterSensor] = sensorDS.assignAscendingTimestamps(_.ts*1000L)

        //keyBy
        val keyByDS: KeyedStream[WaterSensor, String] = markDS.keyBy(_.id)

        //process函数
        val processDS: DataStream[String] = keyByDS.process(
//            new KeyedProcessFunction[String, WaterSensor, String] {
//                override def onTimer(
//                     timestamp: Long,
//                     ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext,
//                     out: Collector[String]): Unit = {
//                    println("timer-----")
//                }
//
//                override def processElement(
//                              value: WaterSensor,
//                              ctx: KeyedProcessFunction[String, WaterSensor, String]#Context,
//                              out: Collector[String]): Unit = {
//                    out.collect("keyedProcess" + ctx.timestamp())
//                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
//                }
//            }
            new MyKeyedProcessFunction01
        )

        processDS.print("process>>>>")

        env.execute("app")
    }
}

class MyKeyedProcessFunction01 extends KeyedProcessFunction[String,WaterSensor,String] {

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        println("timer")
    }

    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
        out.collect(ctx.timestamp().toString)
        //ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime())
        println("currentWaterMark"+ctx.timerService().currentWatermark())
        println("currentProcessing"+ctx.timerService().currentProcessingTime())
        println("timeStamp"+ctx.timestamp())
        ctx.timerService().registerEventTimeTimer(ctx.timestamp()+1000L)
    }
}