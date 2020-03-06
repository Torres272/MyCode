package com.torres.flink.application

import com.torres.flink.common.TApplication
import com.torres.flink.controller.HotItemAnalysesController

object HotItemAnalysesApplication extends App with TApplication{

    //每五分钟统计输出最近一个小时内点击量最多的N个商品
    start{
        val controller = new HotItemAnalysesController
        controller.execute()
    }

}
