package com.torres.flink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object Flink16_API_Sink_ES {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataDS: DataStream[String] = env.readTextFile("input/word.txt")


        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("hadoop102", 9200))


        val wordDS: DataStream[String] = dataDS.flatMap(_.split(" "))

        val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,new ElasticsearchSinkFunction[String] {
            override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                println(s"保存单词:$t")
                val jsonMap = new util.HashMap[String,String]()
                jsonMap.put("data",t)

                val indexRequest: IndexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(jsonMap)
                requestIndexer.add(indexRequest)
                println("保存成功")
            }
        })

//        val ds: DataStream[WaterSensor] = dataDS.map(
//            s => {
//                val datas = s.split(",")
//                WaterSensor(datas(0), datas(1).toLong, datas(2).toDouble)
//            }
//        )

//        val esSinkBuilder = new ElasticsearchSink.Builder[WaterSensor](httpHosts,
//            new ElasticsearchSinkFunction[WaterSensor] {
//                override def process(t: WaterSensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//                    println("saving data: " + t)
//                    val json = new util.HashMap[String, String]()
//                    json.put("data", t.toString)
//                    val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
//                    requestIndexer.add(indexRequest)
//                    println("saved successfully")
//                }
//            })
        // 启动ES时。请使用es用户
        // 访问路径：http://hadoop102:9200/_cat/indices?v
        // 访问路径：http://hadoop102:9200/sensor/_search
        // 访问路径：http://hadoop102:9200/_cat/indices?v
        // 访问路径：http://hadoop102:9200/sensor/_search
        wordDS.addSink(esSinkBuilder.build())


        wordDS.print()
        env.execute()
    }
}
