package com.torres.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object SourceDIYTest {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        env.addSource(new MySource2).print("my=>")

        env.execute("mySource")

    }
}

class MySource extends SourceFunction[String] {
    var flag = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (flag) {
            ctx.collect("random" + new Random().nextInt(9) + 1)
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {
        flag = false
    }
}


class MySource2 extends SourceFunction[String] {
    var flag = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (flag) {
            ctx.collect("random" + (new Random().nextInt(9) + 1))
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = {

    }
}