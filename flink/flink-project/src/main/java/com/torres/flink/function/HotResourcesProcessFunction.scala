package com.torres.flink.function

import java.sql.Timestamp
import java.{lang, util}

import com.torres.flink.bean._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class HotResourcesProcessFunction extends KeyedProcessFunction[Long, HotResourceClick, String] {

    //数据集合
    private var itemList: ListState[HotResourceClick] = _

    //定时器
    private var alarmTimer: ValueState[Long] = _


    override def open(parameters: Configuration): Unit = {
        itemList = getRuntimeContext.getListState(new ListStateDescriptor[HotResourceClick]("itemList", classOf[HotResourceClick]))

        alarmTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTimer", classOf[Long]))
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#OnTimerContext, out: Collector[String]): Unit = {
        val datas: lang.Iterable[HotResourceClick] = itemList.get()
        val dataIter: util.Iterator[HotResourceClick] = datas.iterator()
        val list = new ListBuffer[HotResourceClick]()

        while (dataIter.hasNext) {
            list.append(dataIter.next())
        }
        //清除状态数据
        itemList.clear()
        alarmTimer.clear()

        //处理输出结果
        val result: ListBuffer[HotResourceClick] = list.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

        val builder = new StringBuilder

        builder.append(s"当前时间:${new Timestamp(timestamp)}\n")
        for (elem <- result) {
            builder.append(s"商品：${elem.url},点击数量：${elem.clickCount}\n")
        }
        builder.append("+++++++++++++++++++++++++++")
        out.collect(builder.toString())
        Thread.sleep(1000)
    }

    override def processElement(value: HotResourceClick, ctx: KeyedProcessFunction[Long, HotResourceClick, String]#Context, out: Collector[String]): Unit = {
        itemList.add(value)
        if (alarmTimer.value() == 0) {
            alarmTimer.update(value.windowEndTime)
            ctx.timerService().registerEventTimeTimer(alarmTimer.value())
        }
    }
}
