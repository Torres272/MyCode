package com.torres.flink.controller

import com.torres.flink.common.TController
import com.torres.flink.service.PageViewAnalysesService

class PageViewAnalysesController extends TController{
    private val pageViewAnalysesService = new PageViewAnalysesService

    override def execute(): Unit = {
        val result= pageViewAnalysesService.analyses()
        result.print()
    }
}
