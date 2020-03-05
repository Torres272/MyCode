package com.torres.flink.controller

import com.torres.flink.common.TController
import com.torres.flink.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class HotItemAnalysesController extends TController{
    private val hotItemAnalysesService = new HotItemAnalysesService

    override def execute(): Unit = {
        val result: DataStream[(String, Int)] = hotItemAnalysesService.analyses()
        result.print()
    }
}
