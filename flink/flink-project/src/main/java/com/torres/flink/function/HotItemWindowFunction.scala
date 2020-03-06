package com.torres.flink.function

import com.torres.flink.bean.HotItemClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class HotItemWindowFunction extends WindowFunction[Long,HotItemClick,Long,TimeWindow]{
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[HotItemClick]): Unit = {
        out.collect(HotItemClick(key,input.iterator.next(),window.getEnd))
    }
}
