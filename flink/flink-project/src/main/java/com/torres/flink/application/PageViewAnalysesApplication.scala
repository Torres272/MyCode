package com.torres.flink.application

import com.torres.flink.common.TApplication
import com.torres.flink.controller._

object PageViewAnalysesApplication extends App with TApplication{

    //
    start{
        val controller = new PageViewAnalysesController
        controller.execute()
    }

}
