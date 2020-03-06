package com.torres.flink.controller

import com.torres.flink.common.TController
import com.torres.flink.service.UniqueVisitorAnalysesService

class UniqueVisitorAnalysesController extends TController{
    private val uniqueVisitorAnalysesService = new UniqueVisitorAnalysesService

    override def execute(): Unit = {
        val result= uniqueVisitorAnalysesService.analyses()
        result.print()
    }
}
