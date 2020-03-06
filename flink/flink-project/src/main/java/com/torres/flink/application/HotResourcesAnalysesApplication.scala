package com.torres.flink.application

import com.torres.flink.common.TApplication
import com.torres.flink.controller._

object HotResourcesAnalysesApplication extends App with TApplication{

    //每5秒，输出最近10分钟内访问量最多的N个URL
    start{
        val controller = new HotResourcesAnalysesController
        controller.execute()
    }

}
