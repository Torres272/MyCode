package com.torres.flink.controller

import com.torres.flink.common.TController
import com.torres.flink.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala._

class HotItemAnalysesController extends TController{
    private val hotItemAnalysesService = new HotItemAnalysesService

    override def execute(): Unit = {
        val result: DataStream[String] = hotItemAnalysesService.analyses()
        result.print()
    }
}
