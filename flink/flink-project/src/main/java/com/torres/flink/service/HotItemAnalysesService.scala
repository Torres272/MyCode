package com.torres.flink.service

import com.torres.flink.bean
import com.torres.flink.bean.UserBehavior
import com.torres.flink.common.{TDao, TService}
import com.torres.flink.dao.HotItemAnalysesDao
import com.torres.flink.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemAnalysesService extends TService {

    private val hotItemAnalysesDao = new HotItemAnalysesDao

    override def getDao(): TDao = hotItemAnalysesDao

    override def analyses() = {

        // Todo 1.获取用户行为数据DS
        val dataDS: DataStream[UserBehavior] = getUserBehaviorDatas()

        //Todo 2.抽取时间及设置水位线
        val timeDS: DataStream[UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

        //Todo 3.对数据进行过滤
        val filterDS: DataStream[UserBehavior] = timeDS.filter(_.behavior == "pv")

        //Todo 4.将商品聚合
        val keyedKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

        //Todo 5.开窗
        val dataWS: WindowedStream[UserBehavior, Long, TimeWindow] = keyedKS.timeWindow(Time.hours(1),Time.minutes(5))


        //Todo 6.聚合数据
        val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(
            new HotItemAggregateFunction,
            new HotItemWindowFunction
        )

        //Todo 7.将数据窗口重新分组
        val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)

        //Todo 8.将聚合分组好的数据进行排序
        val result: DataStream[String] = hicKS.process(new HotItemProcessFunction)

        result

    }


}
