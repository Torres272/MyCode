package com.torres.flink

import java.text.SimpleDateFormat

import com.torres.WaterSensor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object flink01_waterMark01 {
    def main(args: Array[String]): Unit = {
        //构建环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        //读取文件
        val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)

        //设置timestamp
        val markDS: DataStream[WaterSensor] = socketDS
          .map(data => {
              val strs: Array[String] = data.split(",")
              WaterSensor(strs(0), strs(1).toLong, strs(2).toDouble)
          })
          .assignTimestampsAndWatermarks(
              new BoundedOutOfOrdernessTimestampExtractor[WaterSensor](Time.seconds(3)) {
                  override def extractTimestamp(element: WaterSensor): Long = {
                      element.ts * 1000L
                  }
              }
          )

        //进行开窗
        val waterMarkDS: DataStream[String] = markDS
          .keyBy(_.id)
          .timeWindow(Time.seconds(5))
          .apply((key, window, datas, out: Collector[String]) => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              out.collect(s"[${window.getStart}-${window.getEnd}],[data:$datas]")
          })


        markDS.print("mark>>>>")
        waterMarkDS.print("water>>>>")


        env.execute("app")
    }
}
