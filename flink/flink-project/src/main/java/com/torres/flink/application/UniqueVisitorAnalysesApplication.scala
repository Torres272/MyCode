package com.torres.flink.application

import com.torres.flink.common.TApplication
import com.torres.flink.controller._

object UniqueVisitorAnalysesApplication extends App with TApplication{

    //PV
    start{
        val controller = new UniqueVisitorAnalysesController
        controller.execute()
    }

}
