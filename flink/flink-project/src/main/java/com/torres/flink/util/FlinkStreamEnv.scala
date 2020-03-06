package com.torres.flink.util


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlinkStreamEnv {

    private val envLocal = new ThreadLocal[StreamExecutionEnvironment]


    def init(): StreamExecutionEnvironment ={
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        envLocal.set(env)
        env

    }

    def get(): StreamExecutionEnvironment ={
        var env: StreamExecutionEnvironment = envLocal.get()
        if(env == null){
            env = init()
        }
        env
    }

    def clear(): Unit ={
        envLocal.remove()
    }

    def execute(): Unit ={
        get().execute("app")
    }
}
