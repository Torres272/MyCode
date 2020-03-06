package com.torres.flink.service

import com.torres.flink.bean.UserBehavior
import com.torres.flink.common.{TDao, TService}
import com.torres.flink.dao.UniqueVisitorAnalysesDao
import com.torres.flink.function.UniqueVisitorAnalysesWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


class UniqueVisitorAnalysesService extends TService {

    private val uniqueVisitorAnalysesDao = new UniqueVisitorAnalysesDao

    override def getDao(): TDao = uniqueVisitorAnalysesDao

    override def analyses() = {
        //PV
        // Todo 1.获取用户行为数据DS
        val dataDS: DataStream[UserBehavior] = getUserBehaviorDatas()

        //Todo 2.抽取时间及设置水位线
        val timeDS: DataStream[UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

        val timeWS: AllWindowedStream[(Long, Int), TimeWindow] = timeDS
          .map(data => (data.userId, 1))
          .timeWindowAll(Time.hours(1))


        timeWS.process(new UniqueVisitorAnalysesWindowFunction)
    }


}
