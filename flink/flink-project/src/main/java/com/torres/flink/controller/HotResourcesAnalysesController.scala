package com.torres.flink.controller

import com.torres.flink.common.TController
import com.torres.flink.service.HotResourcesAnalysesService
import org.apache.flink.streaming.api.scala._

class HotResourcesAnalysesController extends TController{
    private val hotResourcesAnalysesService = new HotResourcesAnalysesService

    override def execute(): Unit = {
        val result: DataStream[String] = hotResourcesAnalysesService.analyses()
        result.print()
    }
}
