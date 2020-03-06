package com.torres.flink.common

import com.torres.flink.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

trait TService {

    def getDao():TDao

    def analyses():Any

    protected def getUserBehaviorDatas()={

        val dataDS: DataStream[String] = getDao.readTextFile("input/UserBehavior.csv")

        dataDS.map(
            data => {
                val strings: Array[String] = data.split(",")
                UserBehavior(
                    strings(0).toLong,
                    strings(1).toLong,
                    strings(2).toLong,
                    strings(3),
                    strings(4).toLong
                )
            }
        )
    }
}
