package com.torres.flink

import com.torres.WaterSensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector


object Flink41_API_Req02 {
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
        val markDS: DataStream[WaterSensor] = sensorDS.assignTimestampsAndWatermarks(
            new AssignerWithPunctuatedWatermarks[WaterSensor] {
                private var currentTS = 0L

                override def checkAndGetNextWatermark(lastElement: WaterSensor, extractedTimestamp: Long): Watermark = {
                    new Watermark(currentTS)
                }

                override def extractTimestamp(element: WaterSensor, previousElementTimestamp: Long): Long = {
                    currentTS = currentTS.max(element.ts * 1000)
                    element.ts * 1000
                }
            }
        )

        //keyBy
        val keyByDS: KeyedStream[WaterSensor, String] = markDS.keyBy(_.id)

        //process函数
        val processDS: DataStream[String] = keyByDS.process(

            new KeyedProcessWaterSensor02
        )

        processDS.print("water>>>>")

        env.execute("app")
    }
}

class KeyedProcessWaterSensor02 extends KeyedProcessFunction[String, WaterSensor, String] {
    private var currentVC: ValueState[Double] = _
    private var alarmTime: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
        currentVC = getRuntimeContext.getState(new ValueStateDescriptor[Double]("currentVC",classOf[Double]))
        alarmTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTime",classOf[Long]))
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(s"传感器：${ctx.getCurrentKey},当前水位：${ctx.timerService().currentWatermark()}连续上升...")
    }

    override def processElement(value: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, String]#Context, out: Collector[String]): Unit = {
        println("sssssss")
        out.collect(value.toString)
        if (value.vc > currentVC.value()) {
            if (alarmTime.value() == 0L) {
                alarmTime.update(value.ts * 1000 + 5000)
                ctx.timerService().registerEventTimeTimer(alarmTime.value())
            }
        } else {
            ctx.timerService().deleteEventTimeTimer(alarmTime.value())
            alarmTime.update(value.ts * 1000 + 5000)
            ctx.timerService().registerEventTimeTimer(alarmTime.value())
        }
        currentVC.update(value.vc)
    }
}