package com.torres.flink.service

import java.text.SimpleDateFormat

import com.torres.flink.bean
import com.torres.flink.bean.ApacheLog
import com.torres.flink.common.{TDao, TService}
import com.torres.flink.dao.HotResourcesAnalysesDao
import com.torres.flink.function.{HotResourcesAggregateFunction, HotResourcesProcessFunction, HotResourcesWindowFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotResourcesAnalysesService extends TService {

    private val hotResourcesAnalysesDao = new HotResourcesAnalysesDao

    override def getDao(): TDao = hotResourcesAnalysesDao

    override def analyses() = {

        //每5秒，输出最近10分钟内访问量最多的N个URL
        // Todo 1.获取用户行为数据DS
        val dataDS: DataStream[String] = hotResourcesAnalysesDao.readTextFile("input/apache.log")

        val logDS: DataStream[ApacheLog] = dataDS.map(
            data => {
                val datas: Array[String] = data.split(" ")
                val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
                ApacheLog(
                    datas(0),
                    datas(1),
                    sdf.parse(datas(3)).getTime,
                    datas(5),
                    datas(6)
                )
            }
        )

        //Todo 2.抽取时间及设置水位线
        val timeDS: DataStream[ApacheLog] = logDS.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
                override def extractTimestamp(element: ApacheLog): Long = {
                    element.eventTime
                }
            }
        )

        //Todo 4.根据URL进行分组
        val keyedKS: KeyedStream[ApacheLog, String] = timeDS.keyBy(_.url)

        //Todo 5.开窗
        val dataWS :WindowedStream[ApacheLog,String,TimeWindow]=keyedKS.timeWindow(Time.minutes(10),Time.seconds(5))



        //Todo 6.聚合数据
        val hicDS = dataWS.aggregate(
            new HotResourcesAggregateFunction,
            new HotResourcesWindowFunction
        )

        //Todo 7.将数据窗口重新分组
        val hicKS: KeyedStream[bean.HotResourceClick, Long] = hicDS.keyBy(_.windowEndTime)

        //Todo 8.将聚合分组好的数据进行排序
        val result: DataStream[String] = hicKS.process(new HotResourcesProcessFunction)

        result

    }


}
