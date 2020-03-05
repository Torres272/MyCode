package com.torres.flink.common

import com.torres.flink.util.FlinkStreamEnv

trait TDao {

    def readTextFile(implicit path: String)={
        FlinkStreamEnv.get().readTextFile(path)
    }

    def readKafka(): Unit = {

    }

    def readSocket(): Unit = {

    }
}
