package com.torres.flink.service

import com.torres.flink.bean.UserBehavior
import com.torres.flink.common.{TDao, TService}
import com.torres.flink.dao.PageViewAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


class PageViewAnalysesService extends TService {

    private val pageViewAnalysesDao = new PageViewAnalysesDao

    override def getDao(): TDao = pageViewAnalysesDao

    override def analyses() = {
        //PV
        // Todo 1.获取用户行为数据DS
        val dataDS: DataStream[UserBehavior] = getUserBehaviorDatas()

        //Todo 2.抽取时间及设置水位线
        val timeDS: DataStream[UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

        //Todo 3.对数据进行过滤
        val filterDS: DataStream[UserBehavior] = timeDS.filter(_.behavior == "pv")
        val pvToOneDS: DataStream[(String, Int)] = filterDS.map(pv =>("pv",1))

        val timeWS: AllWindowedStream[(String, Int), TimeWindow] = pvToOneDS.timeWindowAll(Time.hours(1))

        val result: DataStream[(String, Int)] = timeWS.sum(1)


        result

    }


}
