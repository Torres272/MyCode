package com.torres.flink.service

import com.torres.flink.common.TService
import com.torres.flink.dao.HotItemAnalysesDao
import org.apache.flink.streaming.api.scala._

class HotItemAnalysesService extends TService{

    private val hotItemAnalysesDao = new HotItemAnalysesDao

    override def analyses():  DataStream[(String, Int)] = {
        val dataDS: DataStream[String] = hotItemAnalysesDao.readTextFile("input/word.txt")

        val resultDS: DataStream[(String, Int)] = dataDS
          .flatMap(_.split(" "))
          .map((_, 1))
          .keyBy(_._1)
          .sum(1)


        resultDS
    }
}
