package com.torres.flink.common

import com.torres.flink.util.FlinkStreamEnv

trait TApplication {
    def start(op: =>Unit){
        try{
            FlinkStreamEnv.init()
            op
            FlinkStreamEnv.execute()
        }catch {
            case e =>e.printStackTrace()
        }finally {
            FlinkStreamEnv.clear()
        }
    }
}
