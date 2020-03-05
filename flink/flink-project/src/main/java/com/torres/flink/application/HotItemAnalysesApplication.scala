package com.torres.flink.application

import com.torres.flink.common.TApplication
import com.torres.flink.controller.HotItemAnalysesController

object HotItemAnalysesApplication extends App with TApplication{

    start{
        val controller = new HotItemAnalysesController
        controller.execute()
    }

}
